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
import java.util.Arrays;
import java.util.Collection;
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
import voldemort.store.FailingStore;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.async.AbstractAsynchronousStoreTest;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * A base-socket store test that works with any store RequestFormat
 * 
 * 
 */
public abstract class AbstractSocketStoreTest extends AbstractAsynchronousStoreTest {

    private static final Logger logger = Logger.getLogger(AbstractSocketStoreTest.class);

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
    private SocketStoreFactory socketStoreFactory;
    private StoreRepository repository;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = ServerTestUtils.getStores("test",
                                               VoldemortTestConstants.getOneNodeClusterXml(),
                                               VoldemortTestConstants.getSimpleStoreDefinitionsXml());
        this.socketPort = ServerTestUtils.findFreePort();
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
        socketService = ServerTestUtils.getSocketService(useNio,
                                                         VoldemortTestConstants.getOneNodeClusterXml(),
                                                         VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                         socketPort,
                                                         repository);
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
            local = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(name);
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
            local = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(name);
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

}
