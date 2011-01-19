/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.socket;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.FailingStore;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.AbstractAsynchronousStoreTest;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.memory.InMemoryStore;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.versioned.VersionIncrementingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A base-socket store test that works with any store RequestFormat
 * 
 * 
 */
public abstract class AbstractSocketStoreTest extends AbstractAsynchronousStoreTest {

    private final Logger logger = Logger.getLogger(getClass());

    public AbstractSocketStoreTest(RequestFormatType type, boolean useNio) {
        super("test");
        this.requestFormatType = type;
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { RequestFormatType.VOLDEMORT_V3, true },
                { RequestFormatType.VOLDEMORT_V3, false } });
    }

    private int socketPort;
    private AbstractSocketService socketService;
    protected final RequestFormatType requestFormatType;
    private final boolean useNio;
    private ClientRequestExecutorPool socketStoreFactory;
    private StoreRepository repository;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        int clientConnections = 20;
        int socketBufferSize = 32 * 1024;
        repository = ServerTestUtils.getStores("test",
                                               VoldemortTestConstants.getOneNodeClusterXml(),
                                               VoldemortTestConstants.getSimpleStoreDefinitionsXml());
        this.socketPort = ServerTestUtils.findFreePort();
        socketStoreFactory = new ClientRequestExecutorPool(clientConnections,
                                                           0,
                                                           1000,
                                                           socketBufferSize);
        RequestHandlerFactory factory = ServerTestUtils.getSocketRequestHandlerFactory(VoldemortTestConstants.getOneNodeClusterXml(),
                                                                                       VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                                                       repository);
        socketService = ServerTestUtils.getSocketService(useNio,
                                                         factory,
                                                         socketPort,
                                                         clientConnections,
                                                         clientConnections * 2,
                                                         socketBufferSize);
        socketService.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        socketService.stop();
        socketStoreFactory.close();
    }

    @Override
    public AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(String name) {
        Store<ByteArray, byte[], byte[]> local = repository.getLocalStore(name);
        if(local == null) {
            local = new InMemoryStore<ByteArray, byte[], byte[]>(name);
            repository.addLocalStore(local);
            repository.addRoutedStore(local);
        }
        return ServerTestUtils.getSocketStore(socketStoreFactory,
                                              name,
                                              socketPort,
                                              requestFormatType);
    }

    @Override
    public Store<ByteArray, byte[], byte[]> createStore(String name) {
        return AsyncUtils.asStore(createAsyncStore(name));
    }

    @Override
    public AsynchronousStore<ByteArray, byte[], byte[]> createSlowStore(String name, long delay) {
        Store<ByteArray, byte[], byte[]> local = repository.getLocalStore(name);
        if(local == null) {
            local = new InMemoryStore<ByteArray, byte[], byte[]>(name);
            local = new SleepyStore<ByteArray, byte[], byte[]>(delay, local);
            repository.addLocalStore(local);
            repository.addRoutedStore(local);
        }
        return ServerTestUtils.getSocketStore(socketStoreFactory,
                                              name,
                                              socketPort,
                                              requestFormatType);
    }

    @Override
    public AsynchronousStore<ByteArray, byte[], byte[]> createFailingStore(String name,
                                                                           VoldemortException ex) {
        Store<ByteArray, byte[], byte[]> local = repository.getLocalStore(name);
        if(local == null) {
            local = FailingStore.asStore(name, ex);
            repository.addLocalStore(local);
            repository.addRoutedStore(local);
        }
        return ServerTestUtils.getSocketStore(socketStoreFactory,
                                              name,
                                              socketPort,
                                              requestFormatType);
    }

    @Override
    protected boolean supportsSizes(int keySize, int valueSize) {
        return valueSize < (48 * 1024 * 1024);
    }

    @Test
    public void testVeryLargeValues() throws Exception {
        final AsynchronousStore<ByteArray, byte[], byte[]> store = this.getAsyncStore(storeName);
        byte[] biggie = new byte[1 * 1024 * 1024];
        ByteArray key = new ByteArray(biggie);
        Random rand = new Random();
        for(int i = 0; i < 10; i++) {
            rand.nextBytes(biggie);
            Versioned<byte[]> versioned = new Versioned<byte[]>(biggie);
            this.waitForCompletion(store.submitPut(key, versioned, null));
            assertNotNull(waitForCompletion(store.submitGet(key, null)));
            assertTrue(waitForCompletion(store.submitDelete(key, versioned.getVersion())));
        }
    }

    @Test
    public void testThreadOverload() throws Exception {
        final AsynchronousStore<ByteArray, byte[], byte[]> store = getAsyncStore(storeName);
        int numOps = 100;
        final CountDownLatch latch = new CountDownLatch(numOps);
        Executor exec = Executors.newCachedThreadPool();
        for(int i = 0; i < numOps; i++) {
            exec.execute(new Runnable() {

                public void run() {
                    waitForCompletion(store.submitPut(TestUtils.toByteArray(TestUtils.randomString("abcdefghijklmnopqrs",
                                                                                                   10)),
                                                      new Versioned<byte[]>(TestUtils.randomBytes(8)),
                                                      null));
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    @SuppressWarnings("unused")
    protected boolean performLongTest(int numThreads, int valueSize, int numOps) {
        return false;
    }

    protected void testLotsOfGets(final String testName,
                                  final int numThreads,
                                  final int valueSize,
                                  final int every,
                                  final int numOps) throws Exception {
        if(!performLongTest(numThreads, valueSize, numOps)) {
            logger.info("Skipping test " + testName);
            return;
        }
        logger.info("Starting " + numOps + " gets in " + numThreads + " of size " + valueSize);
        final AsynchronousStore<ByteArray, byte[], byte[]> store = getAsyncStore(storeName);
        final List<ByteArray> keys = this.getKeys(numThreads);
        List<byte[]> values = getByteValues(numThreads, valueSize);
        List<Version> versions = new ArrayList<Version>();
        for(int i = 0; i < numThreads; i++) {
            versions.add(waitForCompletion(store.submitPut(keys.get(i),
                                                           new Versioned<byte[]>(values.get(i)),
                                                           null)));

        }
        final CountDownLatch latch = new CountDownLatch(numThreads);
        Executor exec = Executors.newFixedThreadPool(numThreads);
        long submitted = System.currentTimeMillis();
        for(final ByteArray key: keys) {
            exec.execute(new Runnable() {

                public void run() {
                    long startTime = System.currentTimeMillis();
                    for(int j = 1; j <= numOps; j++) {
                        waitForCompletion(store.submitGet(key, null));
                        if(every > 0 && (j % every) == 0) {
                            long completed = (System.currentTimeMillis() - startTime);
                            logger.info("Completed " + j + " gets in " + completed + " ms "
                                        + (completed / j));
                        }
                    }
                    long completed = System.currentTimeMillis() - startTime;
                    logger.info("Completed " + numOps + " in " + completed + " ms "
                                + (completed / numOps));
                    latch.countDown();
                }
            });
        }
        latch.await();
        for(int i = 0; i < numThreads; i++) {
            waitForCompletion(store.submitDelete(keys.get(i), versions.get(i)));
        }
        logger.info("Completed " + numOps + " gets in " + numThreads + " threads of size "
                    + valueSize + " in " + (System.currentTimeMillis() - submitted) + " ms.");
    }

    @Test
    public void testLotsOfSmallGets() throws Exception {
        testLotsOfGets("LotsOfSmallGets", 10, 1024, 100000, 10000000);
    }

    @Test
    public void testLotsOfBigGets() throws Exception {
        testLotsOfGets("LotsOfBigGets", 10, 1024 * 1024, 500, 10000);
    }

    protected void testLotsOfPuts(final String testName,
                                  final int numThreads,
                                  final int valueSize,
                                  final int every,
                                  final int numOps) throws Exception {
        if(!performLongTest(numThreads, valueSize, numOps)) {
            logger.info("Skipping test " + testName);
            return;
        }
        logger.info("Starting " + numOps + " puts in " + numThreads + " of size " + valueSize);
        AsynchronousStore<ByteArray, byte[], byte[]> async = getAsyncStore(storeName);
        Store<ByteArray, byte[], byte[]> sync = AsyncUtils.asStore(async);
        final Store<ByteArray, byte[], byte[]> store = new VersionIncrementingStore<ByteArray, byte[], byte[]>(sync,
                                                                                                               1,
                                                                                                               SystemTime.INSTANCE);
        final List<ByteArray> keys = this.getKeys(numThreads);
        final List<byte[]> values = getByteValues(numThreads, valueSize);
        final CountDownLatch latch = new CountDownLatch(numThreads);
        Executor exec = Executors.newFixedThreadPool(numThreads);
        long submitted = System.currentTimeMillis();
        for(int i = 0; i < numThreads; i++) {
            final ByteArray key = keys.get(i);
            final byte[] value = values.get(i);
            exec.execute(new Runnable() {

                public void run() {
                    Versioned<byte[]> versioned = new Versioned<byte[]>(value);
                    long startTime = System.currentTimeMillis();
                    for(int j = 1; j <= numOps; j++) {
                        Version version = store.put(key, versioned, null);
                        versioned = new Versioned<byte[]>(value, version);
                        if(every > 0 && (j % every) == 0) {
                            long completed = (System.currentTimeMillis() - startTime);
                            logger.info("Completed " + j + " puts in " + completed + " ms "
                                        + (completed / j));
                        }
                    }
                    long completed = (System.currentTimeMillis() - startTime);
                    logger.info("Completed " + numOps + " puts in " + completed + " ms "
                                + (completed / numOps));
                    latch.countDown();
                }
            });
        }
        latch.await();
        logger.info("Completed " + numOps + " puts in " + numThreads + " threads of size "
                    + valueSize + " in " + (System.currentTimeMillis() - submitted) + " ms.");

    }

    @Test
    public void testLotsOfSmallPuts() throws Exception {
        // 10M puts of 1K size in 10 threads
        testLotsOfPuts("LotsOfSmallPuts", 10, 1024, 100000, 10000000);
    }

    @Test
    public void testLotsOfBigPuts() throws Exception {
        // 10K 1puts of 1M in 10 threads
        testLotsOfPuts("LotsOfBigPuts", 10, 1024 * 1024, 1000, 10000);
    }

    @Test
    public void testRepeatedClosedConnections() throws Exception {
        for(int i = 0; i < 100; i++) {
            Socket s = new Socket();
            s.setTcpNoDelay(true);
            s.setSoTimeout(1000);
            s.connect(new InetSocketAddress("localhost", socketPort));
            logger.info("Client opened" + i);
            // Thread.sleep(1);
            assertTrue(s.isConnected());
            assertTrue(s.isBound());
            assertTrue(!s.isClosed());
            s.close();
            logger.info("Client closed" + i);
        }
    }

    @Test
    public void testNoServerRunning() {
        this.socketService.stop();
        ByteArray key = getKey();
        byte[] value = getValue();
        try {
            doPut(key, value);
            fail("Expected exception");
        } catch(VoldemortException e) {
            assertEquals("Unexpected exception", UnreachableStoreException.class, e.getClass());
        }
    }
}
