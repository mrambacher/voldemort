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
package voldemort.store.routed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.FailingStore;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class DistributedStoreTest extends TestCase {

    private DistributingStore<Node> store;
    Map<Node, Store<ByteArray, byte[]>> inner;
    List<Store<ByteArray, byte[]>> goodStores;
    VectorClockInconsistencyResolver<byte[]> resolver;
    int required;
    int preferred;

    public DistributedStoreTest(int good,
                                int failed,
                                int sleepy,
                                int preferred,
                                int required,
                                long timeout) {
        this.required = required;
        this.preferred = preferred;
        int nodeId = 1;
        VoldemortException oops = new VoldemortException("Oops");
        resolver = new VectorClockInconsistencyResolver<byte[]>();
        inner = new HashMap<Node, Store<ByteArray, byte[]>>(good + failed + sleepy);
        goodStores = new ArrayList<Store<ByteArray, byte[]>>(good);
        for(int i = 0; i < failed; i++) {
            inner.put(new Node(nodeId,
                               "failed_" + nodeId,
                               6666,
                               6666,
                               6667,
                               Lists.newArrayList(nodeId - 1)),
                      new FailingStore<ByteArray, byte[]>("test", oops));
            nodeId++;
        }
        for(int i = 0; i < sleepy; i++) {
            inner.put(new Node(nodeId,
                               "sleepy_" + nodeId,
                               6666,
                               6666,
                               6667,
                               Lists.newArrayList(nodeId - 1)),
                      new SleepyStore<ByteArray, byte[]>(timeout,
                                                         new InMemoryStorageEngine<ByteArray, byte[]>("test")));
            nodeId++;
        }
        for(int i = 0; i < good; i++) {
            Store<ByteArray, byte[]> memory = new InMemoryStorageEngine<ByteArray, byte[]>("test");
            inner.put(new Node(nodeId,
                               "good_" + nodeId,
                               6666,
                               6666,
                               6667,
                               Lists.newArrayList(nodeId - 1)), memory);
            goodStores.add(memory);
            nodeId++;
        }
        store = new DistributingStore<Node>("test",
                                            inner,
                                            Executors.newFixedThreadPool(inner.size()),
                                            preferred,
                                            required,
                                            preferred,
                                            required,
                                            timeout,
                                            TimeUnit.MILLISECONDS,
                                            SystemTime.INSTANCE);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { 3, 0, 0, 3, 3, 100 }, // Expect
                // 3, all
                // good
                { 3, 1, 0, 3, 3, 100 }, // Expect 3 with one bad one in the mix
                { 3, 1, 0, 4, 3, 100 }, // Expect 3, prefer 4 with one bad one
                { 3, 1, 0, 4, 4, 100 }, // Expect 4, prefer 4 with one bad one
                { 1, 2, 2, 4, 4, 1000 }, // Expect 4, prefer 4 with one bad one
        });
    }

    @SuppressWarnings("unused")
    protected void checkStores(ByteArray key, Versioned<byte[]> expected, Version version) {
        for(Store<ByteArray, byte[]> good: goodStores) {
            try {
                List<Versioned<byte[]>> actual = good.get(key);
                assertEquals("Values Match", expected, actual.get(0));
            } catch(VoldemortException e) {
                fail("Unexpected exception " + e.getMessage());
            }
        }
    }

    @Test
    public void testPut() {
        try {
            ByteArray key = new ByteArray("key".getBytes());
            Versioned<byte[]> value = new Versioned<byte[]>(key.get());
            Version version = store.put(key, value);
            checkStores(key, value, version);
        } catch(Exception e) {
            assertEquals("Unexpected Exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getClass());
        }
    }

    protected void checkStores(ByteArray key, List<Versioned<byte[]>> actual) {
        for(Store<ByteArray, byte[]> good: goodStores) {
            try {
                List<Versioned<byte[]>> expected = good.get(key);
                assertEquals("Same number of results", expected.size(), actual.size());
                for(Versioned<byte[]> value: expected) {
                    boolean found = false;
                    for(Versioned<byte[]> result: actual) {
                        if(value.getVersion().equals(result.getVersion())) {
                            assertEquals(value, result);
                            found = true;
                        }
                    }
                    if(!found) {
                        fail("Missing expected version: " + value.getVersion());
                    }
                }
            } catch(VoldemortException e) {
                fail("Unexpected exception " + e.getMessage());
            }
        }
    }

    protected void doPut(ByteArray key, Versioned<byte[]> value) {
        for(Store<ByteArray, byte[]> good: goodStores) {
            try {
                good.put(key, value);
            } catch(Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }

    }

    @Test
    public void testGet() {
        ByteArray key = new ByteArray("key".getBytes());
        Versioned<byte[]> value = new Versioned<byte[]>(key.get());
        doPut(key, value);
        try {
            List<Versioned<byte[]>> values = store.get(key);
            checkStores(key, resolver.resolveConflicts(values));
        } catch(Exception e) {
            assertEquals("Unexpected Exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getClass());

        }
    }

    @Test
    public void testDelete() {
        ByteArray key = new ByteArray("key".getBytes());
        Versioned<byte[]> value = new Versioned<byte[]>(key.get());
        doPut(key, value);
        try {
            boolean deleted = store.delete(key, value.getVersion());
            assertTrue("All deleted", deleted);
            for(Store<ByteArray, byte[]> good: goodStores) {
                try {
                    assertEquals("Deleted all", 0, good.get(key).size());
                } catch(VoldemortException e) {
                    fail("Unexpected exception: " + e.getMessage());
                }
            }
        } catch(Exception e) {
            assertEquals("Unexpected Exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getClass());
        }
    }

    @Test
    public void testInterruptedPut() {
        inner = new HashMap<Node, Store<ByteArray, byte[]>>(3);
        for(int nodeId = 0; nodeId < 3; nodeId++) {
            inner.put(new Node(nodeId,
                               "sleepy_" + nodeId,
                               6666,
                               6666,
                               6667,
                               Lists.newArrayList(nodeId)),
                      new SleepyStore<ByteArray, byte[]>(2000,
                                                         new InMemoryStorageEngine<ByteArray, byte[]>("test")));
        }
        store = new DistributingStore<Node>("test",
                                            inner,
                                            Executors.newFixedThreadPool(inner.size()),
                                            3,
                                            3,
                                            3,
                                            3,
                                            2500,
                                            TimeUnit.MILLISECONDS,
                                            SystemTime.INSTANCE);

        ExecutorService threadPool = Executors.newSingleThreadExecutor();
        final Version master = TestUtils.getClock(1, 1);
        Future<Version> child = threadPool.submit(new Callable<Version>() {

            public Version call() {
                try {
                    ByteArray key = new ByteArray("key".getBytes());
                    Versioned<byte[]> value = new Versioned<byte[]>(key.get(), master);
                    Version version = store.put(key, value);
                    return version;
                } catch(Exception e) {
                    fail("Unexpected exception [" + e.getClass() + "]: " + e.getMessage());
                    return null;
                }
            }
        });

        try {
            Thread.sleep(100);
            threadPool.shutdownNow();
            Version version = child.get();
            assertEquals("Put equals master", master, version);
        } catch(Exception e) {
            fail("Unexpected exception [" + e.getClass() + "]: " + e.getMessage());
        }
    }

    @Test
    public void testInterruptedGet() {
        inner = new HashMap<Node, Store<ByteArray, byte[]>>(3);
        Version master = TestUtils.getClock(1, 1);
        final ByteArray key = new ByteArray("key".getBytes());
        Versioned<byte[]> value = new Versioned<byte[]>(key.get(), master);
        for(int nodeId = 0; nodeId < 3; nodeId++) {
            Store<ByteArray, byte[]> memory = new InMemoryStorageEngine<ByteArray, byte[]>("test");
            memory.put(key, value);
            inner.put(new Node(nodeId,
                               "sleepy_" + nodeId,
                               6666,
                               6666,
                               6667,
                               Lists.newArrayList(nodeId)),
                      new SleepyStore<ByteArray, byte[]>(2000, memory));
        }
        store = new DistributingStore<Node>("test",
                                            inner,
                                            Executors.newFixedThreadPool(inner.size()),
                                            3,
                                            3,
                                            3,
                                            3,
                                            2500,
                                            TimeUnit.MILLISECONDS,
                                            SystemTime.INSTANCE);

        ExecutorService threadPool = Executors.newSingleThreadExecutor();
        Future<List<Versioned<byte[]>>> child = threadPool.submit(new Callable<List<Versioned<byte[]>>>() {

            public List<Versioned<byte[]>> call() throws Exception {
                try {
                    List<Versioned<byte[]>> results = store.get(key);
                    fail("Unexpected results " + results);
                    return results;
                } catch(Exception e) {
                    assertEquals("Unexpected exception",
                                 InsufficientSuccessfulNodesException.class,
                                 e.getClass());
                    throw e;
                }
            }
        });

        try {
            Thread.sleep(100);
            threadPool.shutdownNow();
            List<Versioned<byte[]>> results = child.get();
            fail("Unexpected results " + results);
        } catch(ExecutionException e) {
            assertEquals("Unexpected exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getCause().getClass());
        } catch(Exception e) {
            assertEquals("Unexpected exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getClass());
        }
    }
}
