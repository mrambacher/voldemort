/*
 * Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.store.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.FailingStore;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.async.AbstractAsynchronousStoreTest;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.CallableStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.ThreadedStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class DistributedStoreTest extends AbstractAsynchronousStoreTest {

    ThreadPoolExecutor threadPool;
    Map<String, List<Store<ByteArray, byte[]>>> goodStores;
    private StoreDefinition storeDefinition;
    private final DistributedStore<Node, ByteArray, byte[]> distributedStore;
    private final VoldemortException failure;

    public DistributedStoreTest(int good,
                                int failed,
                                int sleepy,
                                int preferred,
                                int required,
                                long timeout,
                                VoldemortException ex) {
        super("threaded");
        this.failure = ex;
        storeDefinition = buildStoreDef("threaded", good + failed + sleepy, preferred, required);
        threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(this.storeDefinition.getReplicationFactor() * 3);
        distributedStore = this.buildDistributedStore("threaded", good, failed, sleepy, timeout, ex);
    }

    protected StoreDefinitionBuilder getStoreDefBuilder(String name,
                                                        int replicas,
                                                        int preferred,
                                                        int required) {
        StoreDefinitionBuilder storeDefBuilder = new StoreDefinitionBuilder();
        storeDefBuilder.setName(name);
        storeDefBuilder.setType("distributed");
        storeDefBuilder.setReplicationFactor(replicas);
        storeDefBuilder.setRequiredReads(required);
        storeDefBuilder.setRequiredWrites(required);
        storeDefBuilder.setPreferredReads(preferred);
        storeDefBuilder.setPreferredWrites(preferred);
        storeDefBuilder.setKeySerializer(new SerializerDefinition("string"));
        storeDefBuilder.setValueSerializer(new SerializerDefinition("string"));
        storeDefBuilder.setRoutingPolicy(RoutingTier.CLIENT);
        storeDefBuilder.setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY);
        return storeDefBuilder;
    }

    private StoreDefinition buildStoreDef(String name, int replicas, int preferred, int required) {
        StoreDefinitionBuilder storeDefBuilder = getStoreDefBuilder(name,
                                                                    replicas,
                                                                    preferred,
                                                                    required);
        return storeDefBuilder.build();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
        // 3 good ones, 0+0 failures and sleepy. Want/Require all of them
                { 3, 0, 0, 3, 3, 100, null },
                // 3 good ones, 1+0 failures and sleepy. Want 4 but settle for 3
                { 3, 1, 0, 4, 3, 100, new VoldemortException("oops") },
                // No good ones, 2+2 failures/sleepy. Want 4 but settle for 2
                { 0, 2, 2, 4, 2, 100, new VoldemortException("oops") } });
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            threadPool.purge();
            for(int pass = 0; threadPool.getActiveCount() > 0 && pass < 4; pass++) {
                if(!threadPool.awaitTermination(500, TimeUnit.MILLISECONDS)) {}
            }
        } catch(Exception e) {

        }
        super.tearDown();
    }

    public static <K, V> AsynchronousStore<K, V> toThreadedStore(Store<K, V> sync,
                                                                 ExecutorService threadPool) {
        CallableStore<K, V> callable = AsyncUtils.asCallable(sync);
        AsynchronousStore<K, V> threaded = new ThreadedStore<K, V>(callable, threadPool);
        return threaded;
    }

    protected Node makeNode(int id) {
        return new Node(id, "Node_" + id, 80, 81, 82, Collections.singletonList(id));
    }

    private Map<Node, AsynchronousStore<ByteArray, byte[]>> buildNodeStores(String name,
                                                                            int good,
                                                                            int failed,
                                                                            int sleepy,
                                                                            long timeout,
                                                                            VoldemortException ex) {
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < failed + good + sleepy; i++) {
            nodes.add(makeNode(i));
        }
        Cluster cluster = new Cluster(name, nodes);
        return buildNodeStores(cluster, name, good, failed, sleepy, timeout, this.threadPool, ex);

    }

    public static Map<Node, AsynchronousStore<ByteArray, byte[]>> buildNodeStores(Cluster cluster,
                                                                                  String name,
                                                                                  int good,
                                                                                  int failed,
                                                                                  int sleepy,
                                                                                  long timeout,
                                                                                  ExecutorService threadPool,
                                                                                  VoldemortException ex) {
        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = Maps.newHashMap();
        for(int i = 0; i < failed; i++) {
            stores.put(cluster.getNodeById(i),
                       new ThreadedStore<ByteArray, byte[]>(new FailingStore<ByteArray, byte[]>(name
                                                                                                        + "_"
                                                                                                        + i,
                                                                                                ex),
                                                            threadPool));
        }
        for(int i = 0; i < sleepy + good; i++) {
            Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>(name
                                                                                          + "_"
                                                                                          + (i + failed));
            if(i < sleepy) {
                store = new SleepyStore<ByteArray, byte[]>(timeout, store);
            } else {

            }
            stores.put(cluster.getNodeById(i + failed), toThreadedStore(store, threadPool));
        }
        return stores;
    }

    protected DistributedStore<Node, ByteArray, byte[]> buildDistributedStore(String name,
                                                                              int good,
                                                                              int failed,
                                                                              int sleepy,
                                                                              long timeout,
                                                                              VoldemortException ex) {

        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = buildNodeStores(name,
                                                                                 good,
                                                                                 failed,
                                                                                 sleepy,
                                                                                 timeout,
                                                                                 ex);
        return buildDistributedStore(stores, storeDefinition);
    }

    protected DistributedStore<Node, ByteArray, byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[]>> stores,
                                                                              StoreDefinition storeDef) {
        return new DistributedParallelStore<Node, ByteArray, byte[]>(stores, storeDef, true);
    }

    @Override
    public AsynchronousStore<ByteArray, byte[]> createAsyncStore(String name) {
        return DistributedStoreFactory.asAsync(storeDefinition, distributedStore);
    }

    @Override
    protected AsynchronousStore<ByteArray, byte[]> createSlowStore(String name, long delay) {
        return DistributedStoreFactory.asAsync(storeDefinition,
                                               buildDistributedStore(name,
                                                                     0,
                                                                     0,
                                                                     storeDefinition.getReplicationFactor(),
                                                                     delay,
                                                                     null));
    }

    @Override
    protected AsynchronousStore<ByteArray, byte[]> createFailingStore(String name,
                                                                      VoldemortException ex) {
        return DistributedStoreFactory.asAsync(storeDefinition,
                                               buildDistributedStore(name,
                                                                     0,
                                                                     storeDefinition.getReplicationFactor(),
                                                                     0,
                                                                     0,
                                                                     ex));
    }

    @Override
    protected Version checkForObsoleteVersion(Version clock,
                                              int count,
                                              ByteArray key,
                                              Versioned<byte[]> versioned,
                                              VoldemortException e) {
        Version version = null;
        if(e instanceof ObsoleteVersionException) {
            version = super.checkForObsoleteVersion(clock, count, key, versioned, e);
        } else if(e instanceof InsufficientOperationalNodesException) {
            InsufficientOperationalNodesException ione = (InsufficientOperationalNodesException) e;
            for(Throwable cause: ione.getCauses()) {
                if(cause instanceof VoldemortException) {
                    if(!cause.equals(failure)) {
                        version = super.checkForObsoleteVersion(clock,
                                                                count,
                                                                key,
                                                                versioned,
                                                                (VoldemortException) cause);
                    }
                }
            }
        }
        return version;
    }

    @Override
    protected <R> void testFutureTimeout(long timeout, StoreFuture<R> future) {
        testSlowFuture(timeout, future, InsufficientSuccessfulNodesException.class);
    }

    @Override
    protected void checkListenerException(final Class<? extends VoldemortException> expected,
                                          VoldemortException result) {
        assertEquals("Unexcepted exception ",
                     InsufficientSuccessfulNodesException.class,
                     result.getClass());
    }

    private void testFailingFuture(StoreFuture<?> future,
                                   int available,
                                   int required,
                                   int good,
                                   int failures,
                                   Class<? extends VoldemortException> expected) {
        try {
            future.get();
            fail("Expected failure");
        } catch(VoldemortException e) {
            assertEquals("Unexpected exception", expected, e.getClass());
            if(e instanceof InsufficientSuccessfulNodesException) {
                InsufficientSuccessfulNodesException isne = (InsufficientSuccessfulNodesException) e;
                int successful = isne.getSuccessful();
                assertEquals("Available", available, isne.getAvailable());
                assertEquals("Required ", required, isne.getRequired());
                assertEquals("Failures", failures, isne.getCauses().size());
                assertTrue("Successes (" + good + ">=" + successful + ")", good >= successful);
            }
        }

    }

    private void testFailingFuture(StoreFuture<?> future,
                                   int available,
                                   int required,
                                   int good,
                                   int failures) {
        testFailingFuture(future,
                          available,
                          required,
                          good,
                          failures,
                          InsufficientSuccessfulNodesException.class);
    }

    @Test
    public void testFailingStores() {
        // Test that the request fails quickly.
        // Build a store with 1 good, 4 bad, and 1 sleepy, require/prefer 4
        // responses.
        int available = 7;
        int required = 4;
        int failures = 4;
        int good = 0;
        StoreDefinition storeDef = this.buildStoreDef("required", available, required, required);
        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = buildNodeStores("required",
                                                                                 good,
                                                                                 failures,
                                                                                 3,
                                                                                 1000,
                                                                                 new VoldemortException("oops"));
        AsynchronousStore<ByteArray, byte[]> store = DistributedStoreFactory.asAsync(storeDef,
                                                                                     buildDistributedStore(stores,
                                                                                                           storeDef));

        ByteArray key = getKey();
        byte[] value = getValue();

        testFailingFuture(store.submitGet(key), available, required, good, failures);
        testFailingFuture(store.submitPut(key, new Versioned<byte[]>(value)),
                          available,
                          required,
                          good,
                          failures);
        testFailingFuture(store.submitDelete(key, VersionFactory.newVersion()),
                          available,
                          required,
                          good,
                          failures);
        testFailingFuture(store.submitGetVersions(key), available, required, good, failures);
    }

    @Test
    public void testGetAllWithSomeFailures() {
    // testReturnOnRequired(store.submitGetAll(keys), 0, good);
    }

    public static void testIllegalArguments(DistributedStore<Node, ByteArray, byte[]> store,
                                            String reason,
                                            ByteArray key,
                                            byte[] value,
                                            List<Node> nodes,
                                            int preferred,
                                            int required,
                                            Class<? extends Exception> expected) {
        try {
            store.submitGet(key, nodes, preferred, required);
            fail("Get should fail: " + reason);
        } catch(Exception e) {
            assertEquals("Expected exception: " + reason, expected, e.getClass());
        }
        try {
            store.submitDelete(key, VersionFactory.newVersion(), nodes, preferred, required);
            fail("Delete should fail: " + reason);
        } catch(Exception e) {
            assertEquals("Expected exception: " + reason, expected, e.getClass());
        }
        try {
            store.submitGetVersions(key, nodes, preferred, required);
            fail("Get Versions should fail: " + reason);
        } catch(Exception e) {
            assertEquals("Expected exception: " + reason, expected, e.getClass());
        }
        try {
            store.submitPut(key, new Versioned<byte[]>(value), nodes, preferred, required);
            fail("Put should fail: " + reason);
        } catch(Exception e) {
            assertEquals("Expected exception: " + reason, expected, e.getClass());
        }
    }

    @Test
    public void testCheckRequired() {
        ByteArray key = getKey();
        byte[] value = getValue();
        StoreDefinition storeDef = this.buildStoreDef("required", 1, 1, 1);
        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = buildNodeStores("invalid",
                                                                                 1,
                                                                                 0,
                                                                                 0,
                                                                                 0,
                                                                                 null);
        DistributedStore<Node, ByteArray, byte[]> store = buildDistributedStore(stores, storeDef);
        List<Node> nodes = new ArrayList<Node>(stores.keySet());
        // Test that an exception is thrown if there is no list supplied
        int count = nodes.size() + 1;
        testIllegalArguments(store,
                             "Too few required nodes",
                             key,
                             value,
                             nodes,
                             count,
                             count,
                             InsufficientOperationalNodesException.class);
    }

    @Test
    public void testIllegalArguments() {
        ByteArray key = getKey();
        byte[] value = getValue();
        StoreDefinition storeDef = this.buildStoreDef("invalid", 1, 1, 1);
        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = buildNodeStores("invalid",
                                                                                 1,
                                                                                 0,
                                                                                 0,
                                                                                 0,
                                                                                 null);
        DistributedStore<Node, ByteArray, byte[]> store = buildDistributedStore(stores, storeDef);
        List<Node> nodes = new ArrayList<Node>(stores.keySet());
        // Test that an exception is thrown if there is no list supplied
        testIllegalArguments(store,
                             "Null node list",
                             key,
                             value,
                             null,
                             1,
                             1,
                             IllegalArgumentException.class);
        Node dummy = new Node(100, "dummy", 101, 102, 103, Collections.singletonList(100));
        // Test that an exception is thrown if the list contains invalid nodes
        testIllegalArguments(store,
                             "Non-existent node in list",
                             key,
                             value,
                             Collections.singletonList(dummy),
                             1,
                             1,
                             IllegalArgumentException.class);
        // Test that an exception is thrown if preferred < required
        testIllegalArguments(store,
                             "Required > Preferred",
                             key,
                             value,
                             nodes,
                             1,
                             2,
                             IllegalArgumentException.class);
        // Test that an exception is thrown if required < 0
        testIllegalArguments(store,
                             "Required < 0",
                             key,
                             value,
                             nodes,
                             0,
                             -1,
                             IllegalArgumentException.class);

        List<Node> empty = new ArrayList<Node>();
        try {
            store.submitGet(null, empty, 1, 1);
            fail("Get should fail on null key");
        } catch(Exception e) {
            assertEquals("Expected illegal argument", IllegalArgumentException.class, e.getClass());
        }

        try {
            store.submitDelete(null, VersionFactory.newVersion(), empty, 1, 1);
            fail("Delete should fail on null key");
        } catch(Exception e) {
            assertEquals("Expected illegal argument", IllegalArgumentException.class, e.getClass());
        }
        try {
            store.submitDelete(null, VersionFactory.newVersion(), empty, 1, 1);
            fail("Delete should fail on null version");
        } catch(Exception e) {
            assertEquals("Expected illegal argument", IllegalArgumentException.class, e.getClass());
        }

        try {
            store.submitGetAll(null, 1, 1);
            fail("Get All should fail on empty node list");
        } catch(Exception e) {
            assertEquals("Expected illegal argument", IllegalArgumentException.class, e.getClass());
        }

        try {
            store.submitGetVersions(null, empty, 1, 1);
            fail("Get Versions should fail on null key");
        } catch(Exception e) {
            assertEquals("Expected illegal argument", IllegalArgumentException.class, e.getClass());
        }

        try {
            store.submitPut(null, new Versioned<byte[]>(value), empty, 1, 1);
            fail("Put should fail on null key");
        } catch(Exception e) {
            assertEquals("Expected illegal argument", IllegalArgumentException.class, e.getClass());
        }
    }

    @SuppressWarnings("unchecked")
    private <R> void testReturnOnRequired(StoreFuture<R> future, long delay, int expected) {
        try {
            if(delay > 0) {
                future.get(delay, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
            DistributedFuture<?, R> distributedFuture = (DistributedFuture<?, R>) future;
            assertEquals("Good completed successfully", expected, distributedFuture.getCompleted());
        } catch(VoldemortException e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    @Test
    public void testReturnOnRequired() {
        // Test that we return when preferred nodes have completed and do not
        // wait for all of them.
        int good = 2;
        int sleepy = 3;
        StoreDefinition storeDef = this.buildStoreDef("required", good + sleepy, good, good);
        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = buildNodeStores("required",
                                                                                 good,
                                                                                 0,
                                                                                 sleepy,
                                                                                 2000,
                                                                                 null);
        AsynchronousStore<ByteArray, byte[]> store = DistributedStoreFactory.asAsync(storeDef,
                                                                                     buildDistributedStore(stores,
                                                                                                           storeDef));

        ByteArray key = getKey();
        byte[] value = getValue();
        Versioned<byte[]> versioned = new Versioned<byte[]>(value);

        testReturnOnRequired(store.submitGet(key), 0, good);
        testReturnOnRequired(store.submitPut(key, versioned), 0, good);
        testReturnOnRequired(store.submitDelete(key, VersionFactory.newVersion()), 0, good);
        testReturnOnRequired(store.submitGetVersions(key), 0, good);
    }

    @Test
    public void testInvalidMetadata() {
        // Test that an invalid metadata exception is correctly thrown through
        // the Distributed Store layer
        // Test that the request fails quickly.
        // Build a store with 1 good, 4 bad, and 1 sleepy, require/prefer 4
        // responses.
        int available = 6;
        int required = 4;
        int failures = 4;
        int good = 1;
        StoreDefinition storeDef = this.buildStoreDef("required", available, required, required);
        Map<Node, AsynchronousStore<ByteArray, byte[]>> stores = buildNodeStores("required",
                                                                                 good,
                                                                                 failures,
                                                                                 1,
                                                                                 1000,
                                                                                 new InvalidMetadataException("oops"));
        AsynchronousStore<ByteArray, byte[]> store = DistributedStoreFactory.asAsync(storeDef,
                                                                                     buildDistributedStore(stores,
                                                                                                           storeDef));

        ByteArray key = getKey();
        byte[] value = getValue();

        testFailingFuture(store.submitGet(key),
                          available,
                          required,
                          good,
                          failures,
                          InvalidMetadataException.class);
        testFailingFuture(store.submitPut(key, new Versioned<byte[]>(value)),
                          available,
                          required,
                          good,
                          failures,
                          InvalidMetadataException.class);
        testFailingFuture(store.submitDelete(key, VersionFactory.newVersion()),
                          available,
                          required,
                          good,
                          failures,
                          InvalidMetadataException.class);
        testFailingFuture(store.submitGetVersions(key),
                          available,
                          required,
                          good,
                          failures,
                          InvalidMetadataException.class);
    }
}
