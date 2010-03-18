/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright © 2010 Nokia Corporation. All rights reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.niosocket.NioSocketService;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.socket.SocketService;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Tests to test a multi-threaded client against NIO and BIO sockets.
 */

@RunWith(Parameterized.class)
public class ThreadedSocketStoreTest extends TestCase {

    private int socketPort;
    private AbstractSocketService socketService;
    protected final RequestFormatType requestFormatType;
    private final boolean useNio;

    public ThreadedSocketStoreTest(RequestFormatType type, boolean useNio) {
        this.requestFormatType = type;
        this.useNio = useNio;

    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { RequestFormatType.VOLDEMORT_V3, true },
                { RequestFormatType.VOLDEMORT_V3, false } });
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if(socketService != null) {
            socketService.stop();
            socketService = null;
        }
    }

    protected void startSocketService(long sleepMs,
                                      int maxThreads,
                                      int parallelThreads,
                                      int queueLength) {
        this.socketPort = ServerTestUtils.findFreePort();
        StoreRepository repository = ServerTestUtils.getStores("test",
                                                               VoldemortTestConstants.getOneNodeClusterXml(),
                                                               VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                               sleepMs);
        RequestHandlerFactory factory = ServerTestUtils.getSocketRequestHandlerFactory(VoldemortTestConstants.getOneNodeClusterXml(),
                                                                                       VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                                                       repository);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(maxThreads / 2,
                                                               maxThreads,
                                                               0,
                                                               TimeUnit.MILLISECONDS,
                                                               new SynchronousQueue<Runnable>());
        if(useNio) {
            socketService = new NioSocketService(factory,
                                                 threadPool,
                                                 socketPort,
                                                 10000,
                                                 1,
                                                 parallelThreads,
                                                 queueLength,
                                                 "client-request-service",
                                                 false);
        } else {
            socketService = new SocketService(factory,
                                              threadPool,
                                              socketPort,
                                              10000,
                                              queueLength,
                                              "client-request-service",
                                              false);
        }
        socketService.start();

    }

    public Store<ByteArray, byte[]> getStore(int numThreads,
                                             int connectionTimeout,
                                             int socketTimeout) {
        Store<ByteArray, byte[]> store = ServerTestUtils.getSocketStore("test",
                                                                        "localhost",
                                                                        socketPort,
                                                                        requestFormatType,
                                                                        numThreads,
                                                                        connectionTimeout,
                                                                        socketTimeout,
                                                                        false);
        return store;
    }

    @Test
    public void test100ThreadsWaiting100ms() {
        testThreadedStore(100, 100, 300, 100, 100, 100);
    }

    @Test
    public void test500ThreadsWaiting100ms() {
        testThreadedStore(500, 300, 300, 50, 500, 100);
    }

    /**
     * Tests threaded stores
     * 
     * @param numberOfThreads Number of client and server threads
     * @param connectionTimeout How long the client should wait for a socket
     *        connect
     * @param socketTimeout How long the client waits for the server to respond
     * @param queueLength How many listen/bind() operations can be pending
     * @param sleepMs How long the server should delay before responding
     */
    protected void testThreadedStore(int numberOfThreads,
                                     int connectionTimeout,
                                     int socketTimeout,
                                     int parallelThreshold,
                                     int queueLength,
                                     long sleepMs) {
        startSocketService(sleepMs, numberOfThreads, parallelThreshold, queueLength);
        final Store<ByteArray, byte[]> store = getStore(numberOfThreads,
                                                        connectionTimeout,
                                                        socketTimeout);

        final ByteArray key = new ByteArray("1".getBytes());
        final Versioned<byte[]> value = new Versioned<byte[]>(key.get());

        store.put(key, value);
        final List<Exception> failures = new ArrayList<Exception>();
        ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch isDone = new CountDownLatch(numberOfThreads);
        List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(numberOfThreads);

        for(int counter = 0; counter < numberOfThreads; counter++) {
            Runnable task = new Runnable() {

                public void run() {
                    try {
                        for(int pass = 0; pass < 20; pass++) {
                            store.get(key);
                        }
                    } catch(Exception e) {
                        System.out.println("Oops: " + e.getMessage());
                        failures.add(e);
                    } finally {
                        isDone.countDown();
                    }
                    return;
                }
            };
            tasks.add(Executors.callable(task));
        }
        try {
            service.invokeAll(tasks);
            isDone.await();
        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
        assertEquals("No Failures", 0, failures.size());
    }
}
