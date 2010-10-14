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

import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.FailureDetectorTestUtils.recordSuccess;
import static voldemort.MutableStoreVerifier.create;
import static voldemort.TestUtils.getClock;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.FailingReadsStore;
import voldemort.store.FailingStore;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.Tracked;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Basic tests for RoutedStore
 * 
 * 
 */
@RunWith(Parameterized.class)
public class RoutedStoreTest extends AbstractByteArrayStoreTest {

    private Cluster cluster;
    private final ByteArray aKey = TestUtils.toByteArray("jay");
    private final byte[] aValue = "kreps".getBytes();
    private final Class<FailureDetector> failureDetectorClass;
    private final boolean isPipelineRoutedStoreEnabled;
    private FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;

    public RoutedStoreTest(Class<FailureDetector> failureDetectorClass,
                           boolean isPipelineRoutedStoreEnabled) {
        super("test");
        this.failureDetectorClass = failureDetectorClass;
        this.isPipelineRoutedStoreEnabled = isPipelineRoutedStoreEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        cluster = getNineNodeCluster();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

        if(failureDetector != null)
            failureDetector.destroy();

        if(routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class, true },
                { BannagePeriodFailureDetector.class, false } });
    }

    @Override
    public Store<ByteArray, byte[]> createStore(String name) {
        return createStore(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 4, 0, 0);
    }

    public Store<ByteArray, byte[]> createStore(int reads,
                                                int writes,
                                                int threads,
                                                int failing,
                                                int sleepy) {
        return new InconsistencyResolvingStore<ByteArray, byte[]>(getStore(cluster,
                                                                           reads,
                                                                           writes,
                                                                           threads,
                                                                           failing,
                                                                           sleepy),
                                                                  new VectorClockInconsistencyResolver<byte[]>());
    }

    public Store<ByteArray, byte[]> createStore(Cluster cluster,
                                                Map<Integer, Store<ByteArray, byte[]>> subStores,
                                                StoreDefinition storeDef,
                                                int threads) {
        RoutedStore routedStore = createRoutedStore(cluster, storeDef, threads, subStores);
        return new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                  new VectorClockInconsistencyResolver<byte[]>());
    }

    private RoutedStore getStore(Cluster cluster,
                                 int reads,
                                 int writes,
                                 int threads,
                                 int failing,
                                 int sleepy) {
        return getStore(cluster,
                        reads,
                        writes,
                        threads,
                        failing,
                        sleepy,
                        RoutingStrategyType.TO_ALL_STRATEGY,
                        new VoldemortException());
    }

    public static Map<Integer, Store<ByteArray, byte[]>> createClusterStores(Cluster cluster) {
        return createClusterStores(cluster, "test", 0, 0, null);
    }

    public static Map<Integer, Store<ByteArray, byte[]>> createClusterStores(Cluster cluster,
                                                                             String storeName,
                                                                             int failing,
                                                                             int sleepy,
                                                                             VoldemortException e) {
        if(failing + sleepy > cluster.getNumberOfNodes()) {
            throw new IllegalArgumentException(failing + " failing nodes, " + sleepy
                                               + " sleepy nodes, but only "
                                               + cluster.getNumberOfNodes()
                                               + " nodes in the cluster.");
        }
        int count = 0;
        Map<Integer, Store<ByteArray, byte[]>> clusterStores = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            if(count < failing) {
                clusterStores.put(node.getId(), new FailingStore<ByteArray, byte[]>(storeName, e));
            } else if(count < failing + sleepy) {
                clusterStores.put(node.getId(),
                                  new SleepyStore<ByteArray, byte[]>(Long.MAX_VALUE,
                                                                     new InMemoryStorageEngine<ByteArray, byte[]>("test")));
            } else {
                clusterStores.put(node.getId(),
                                  new InMemoryStorageEngine<ByteArray, byte[]>(storeName
                                                                               + node.getId()));
            }
            count++;
        }
        return clusterStores;
    }

    private RoutedStore getStore(Cluster cluster,
                                 int reads,
                                 int writes,
                                 int threads,
                                 int failing,
                                 int sleepy,
                                 String strategy,
                                 VoldemortException e) {
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        Map<Integer, Store<ByteArray, byte[]>> clusterStores = createClusterStores(cluster,
                                                                                   "test",
                                                                                   failing,
                                                                                   sleepy,
                                                                                   e);

        createFailureDetector();
        for(Node n: cluster.getNodes()) {
            Store<ByteArray, byte[]> subStore = clusterStores.get(n.getId());
            subStores.put(n.getId(), new NodeStore<ByteArray, byte[]>(n, failureDetector, subStore));
        }
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               reads + writes,
                                                               reads,
                                                               reads,
                                                               writes,
                                                               writes,
                                                               strategy);
        return createRoutedStore(cluster, storeDef, threads, subStores);
    }

    private RoutedStore createRoutedStore(Cluster cluster,
                                          StoreDefinition storeDef,
                                          int threads,
                                          Map<Integer, Store<ByteArray, byte[]>> subStores) {
        setFailureDetectorVerifier(subStores);
        routedStoreThreadPool = Executors.newFixedThreadPool(threads);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);
        return routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);
    }

    private int countOccurances(RoutedStore routedStore, ByteArray key, Versioned<byte[]> value) {
        int count = 0;
        for(Store<ByteArray, byte[]> store: routedStore.getNodeStores().values())
            try {
                if(store.get(key).size() > 0 && Utils.deepEquals(store.get(key).get(0), value))
                    count += 1;
            } catch(VoldemortException e) {
                // This is normal for the failing store...
            }
        return count;
    }

    private void assertNEqual(RoutedStore routedStore,
                              int expected,
                              ByteArray key,
                              Versioned<byte[]> value) {
        int count = countOccurances(routedStore, key, value);
        assertEquals("Expected " + expected + " occurances of '" + key + "' with value '" + value
                     + "', but found " + count + ".", expected, count);
    }

    private void assertNOrMoreEqual(RoutedStore routedStore,
                                    int expected,
                                    ByteArray key,
                                    Versioned<byte[]> value) {
        int count = countOccurances(routedStore, key, value);
        assertTrue("Expected " + expected + " or more occurances of '" + key + "' with value '"
                   + value + "', but found " + count + ".", expected <= count);
    }

    private void testBasicOperations(int reads, int writes, int failures, int threads)
            throws Exception {
        RoutedStore routedStore = getStore(cluster, reads, writes, threads, failures, 0);
        Store<ByteArray, byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        Version clock = getClock(1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);
        routedStore.put(aKey, versioned);
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);
        List<Versioned<byte[]>> found = store.get(aKey);
        assertEquals(1, found.size());
        assertEquals(versioned, found.get(0));
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);
        assertTrue(routedStore.delete(aKey, versioned.getVersion()));
        assertNEqual(routedStore, 0, aKey, versioned);
        assertTrue(!routedStore.delete(aKey, versioned.getVersion()));
    }

    @Test
    public void testBasicOperationsSingleThreaded() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 0, 1);
    }

    @Test
    public void testBasicOperationsMultiThreaded() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 0, 4);
    }

    @Test
    public void testBasicOperationsMultiThreadedWithFailures() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes() - 2, cluster.getNumberOfNodes() - 2, 2, 4);
    }

    private void checkException(int required,
                                int total,
                                int failures,
                                InsufficientOperationalNodesException ione) {
        assertEquals("Required Count", required, ione.getRequired());
        assertEquals("Available Count", total, ione.getAvailable());
        if(ione instanceof InsufficientSuccessfulNodesException) {
            InsufficientSuccessfulNodesException isne = (InsufficientSuccessfulNodesException) ione;
            assertEquals("Successful Count", total - failures, isne.getSuccessful());
        }
    }

    private void testBasicOperationFailure(int reads, int writes, int failures, int threads)
            throws Exception {
        Version clock = getClock(1);
        int availableForWrite = Math.max(cluster.getNumberOfNodes() - failures, writes - 1);
        int availableForRead = Math.max(cluster.getNumberOfNodes() - failures, reads - 1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);
        RoutedStore routedStore = getStore(cluster,
                                           reads,
                                           writes,
                                           threads,
                                           failures,
                                           0,
                                           RoutingStrategyType.TO_ALL_STRATEGY,
                                           new UnreachableStoreException("no go"));
        try {
            routedStore.put(aKey, versioned);
            fail("Put succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            checkException(writes, availableForWrite, failures, e);
        }
        try {
            routedStore.get(aKey);
            fail("Get succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            checkException(reads, availableForRead, failures, e);
        }
        try {
            routedStore.delete(aKey, versioned.getVersion());
            fail("Get succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            checkException(writes, availableForWrite, failures, e);
        }
    }

    @Test
    public void testBasicOperationFailureMultiThreaded() throws Exception {
        testBasicOperationFailure(cluster.getNumberOfNodes() - 2,
                                  cluster.getNumberOfNodes() - 2,
                                  4,
                                  4);
    }

    @Test
    public void testPutIncrementsVersion() throws Exception {
        Store<ByteArray, byte[]> store = getStore();
        VectorClock clock = new VectorClock();
        VectorClock copy = clock.clone();
        store.put(aKey, new Versioned<byte[]>(getValue(), clock));
        List<Versioned<byte[]>> found = store.get(aKey);
        assertEquals("Invalid number of items found.", 1, found.size());
        assertEquals("Version not incremented properly",
                     Occured.BEFORE,
                     copy.compare(found.get(0).getVersion()));
    }

    @Test
    public void testObsoleteMasterFails() {
    // write me
    }

    @Test
    public void testOnlyNodeFailuresDisableNode() throws Exception {
        // test put
        cluster = getNineNodeCluster();

        Store<ByteArray, byte[]> s1 = getStore(cluster,
                                               1,
                                               9,
                                               9,
                                               9,
                                               0,
                                               RoutingStrategyType.TO_ALL_STRATEGY,
                                               new VoldemortException());
        try {
            s1.put(aKey, new Versioned<byte[]>(aValue));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(9, 8, 9, e);
        }
        assertOperationalNodes(9);

        cluster = getNineNodeCluster();

        Store<ByteArray, byte[]> s2 = getStore(cluster,
                                               9,
                                               1,
                                               9,
                                               9,
                                               0,
                                               RoutingStrategyType.TO_ALL_STRATEGY,
                                               new UnreachableStoreException("no go"));
        try {
            s2.put(aKey, new Versioned<byte[]>(aValue));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 0, 9, e);
        }
        assertOperationalNodes(0);

        // test get
        cluster = getNineNodeCluster();

        s1 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new VoldemortException());
        try {
            s1.get(aKey);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 9, 9, e);
        }
        assertOperationalNodes(9);

        cluster = getNineNodeCluster();

        s2 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new UnreachableStoreException("no go"));
        try {
            s2.get(aKey);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 9, 9, e);
        }
        assertOperationalNodes(0);

        // test delete
        cluster = getNineNodeCluster();

        s1 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new VoldemortException());
        try {
            s1.delete(aKey, new VectorClock());
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(9, 9, 9, e);
        }
        assertOperationalNodes(9);

        cluster = getNineNodeCluster();

        s2 = getStore(cluster,
                      1,
                      1,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new UnreachableStoreException("no go"));
        try {
            s2.delete(aKey, new VectorClock());
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 9, 9, e);
        }
        assertOperationalNodes(0);
    }

    @Test
    public void testGetVersions2() throws Exception {
        List<ByteArray> keys = getKeys(2);
        ByteArray key = keys.get(0);
        byte[] value = getValue();
        Store<ByteArray, byte[]> store = getStore();
        store.put(key, Versioned.value(value));
        List<Versioned<byte[]>> versioneds = store.get(key);
        List<Version> versions = store.getVersions(key);
        assertEquals(1, versioneds.size());
        assertEquals(9, versions.size());
        for(int i = 0; i < versions.size(); i++)
            assertEquals(versioneds.get(0).getVersion(), versions.get(i));

        assertEquals(0, store.getVersions(keys.get(1)).size());
    }

    /**
     * Tests that getAll works correctly with a node down in a two node cluster.
     */
    @Test
    public void testGetAllWithNodeDown() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        RoutedStore routedStore = getStore(cluster, 1, 2, 1, 0, 0);
        Store<ByteArray, byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value));
            expectedValues.put(key, value);
        }

        recordException(failureDetector, cluster.getNodes().iterator().next());

        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet());
        assertEquals(expectedValues.size(), all.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: all.entrySet()) {
            byte[] value = expectedValues.get(mapEntry.getKey());
            assertEquals(new ByteArray(value), new ByteArray(mapEntry.getValue().get(0).getValue()));
        }
    }

    @Test
    public void testGetAllWithFailingStore() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               2,
                                                               1,
                                                               1,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        this.createFailureDetector();

        int id1 = Iterables.get(cluster.getNodes(), 0).getId();
        int id2 = Iterables.get(cluster.getNodes(), 1).getId();
        subStores.put(id1, new InMemoryStorageEngine<ByteArray, byte[]>("test"));
        subStores.put(id2, new FailingReadsStore<ByteArray, byte[]>("test"));

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);
        setFailureDetectorVerifier(subStores);

        Store<ByteArray, byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value));
            expectedValues.put(key, value);
        }

        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet());
        assertEquals(expectedValues.size(), all.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: all.entrySet()) {
            byte[] value = expectedValues.get(mapEntry.getKey());
            assertEquals(new ByteArray(value), new ByteArray(mapEntry.getValue().get(0).getValue()));
        }
    }

    /**
     * One node up, two preferred reads and one required read. See:
     * 
     * http://github.com/voldemort/voldemort/issues#issue/18
     */
    @Test
    public void testGetAllWithMorePreferredReadsThanNodes() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               2,
                                                               2,
                                                               1,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        this.createFailureDetector();

        int id1 = Iterables.get(cluster.getNodes(), 0).getId();
        int id2 = Iterables.get(cluster.getNodes(), 1).getId();
        subStores.put(id1, new InMemoryStorageEngine<ByteArray, byte[]>("test"));
        subStores.put(id2, new InMemoryStorageEngine<ByteArray, byte[]>("test"));

        setFailureDetectorVerifier(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        Store<ByteArray, byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, Versioned.value(aValue));
        recordException(failureDetector, cluster.getNodes().iterator().next());
        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(Arrays.asList(aKey));
        assertEquals(1, all.size());
        assertTrue(Arrays.equals(aValue, all.values().iterator().next().get(0).getValue()));
    }

    /**
     * See Issue #89: Sequential retrieval in RoutedStore.get doesn't consider
     * repairReads.
     */
    @Test
    public void testReadRepairWithFailures() throws Exception {
        cluster = getNineNodeCluster();

        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodes() - 1,
                                           cluster.getNumberOfNodes() - 1,
                                           1,
                                           0,
                                           0);
        // Disable node 1 so that the first put also goes to the last node
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 1));
        Store<ByteArray, byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, new Versioned<byte[]>(aValue));

        byte[] anotherValue = "john".getBytes();

        // Disable the last node and enable node 1 to prevent the last node from
        // getting the new version
        recordException(failureDetector, Iterables.getLast(cluster.getNodes()));
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 1));
        Version clock = getClock(1);
        store.put(aKey, new Versioned<byte[]>(anotherValue, clock));

        // Enable last node and disable node 1, the following get should cause a
        // read repair on the last node in the code path that is only executed
        // if there are failures.
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 1));
        recordSuccess(failureDetector, Iterables.getLast(cluster.getNodes()));
        List<Versioned<byte[]>> versioneds = store.get(aKey);
        assertEquals(1, versioneds.size());
        assertEquals(new ByteArray(anotherValue), new ByteArray(versioneds.get(0).getValue()));

        // Read repairs are done asynchronously, so we sleep for a short period.
        // It may be a good idea to use a synchronous executor service.
        Thread.sleep(100);
        for(Map.Entry<Integer, Store<ByteArray, byte[]>> entry: routedStore.getNodeStores()
                                                                           .entrySet()) {
            recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), entry.getKey()));
            Store<ByteArray, byte[]> inner = entry.getValue();
            List<Versioned<byte[]>> innerVersioneds = inner.get(aKey);
            assertEquals(1, versioneds.size());
            assertEquals(new ByteArray(anotherValue), new ByteArray(innerVersioneds.get(0)
                                                                                   .getValue()));
        }

    }

    /**
     * See issue #134: RoutedStore put() doesn't wait for enough attempts to
     * succeed
     * 
     * This issue would only happen with one node down and another that was slow
     * to respond.
     */
    @Test
    public void testPutWithOneNodeDownAndOneNodeSlow() throws Exception {
        this.createFailureDetector();
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               2,
                                                               2,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        /* The key used causes the nodes selected for writing to be [2, 0, 1] */
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();

        int id1 = Iterables.get(cluster.getNodes(), 0).getId();
        int id2 = Iterables.get(cluster.getNodes(), 1).getId();
        int id3 = Iterables.get(cluster.getNodes(), 2).getId();

        subStores.put(id3, new InMemoryStorageEngine<ByteArray, byte[]>("test"));
        subStores.put(id1, new FailingStore<ByteArray, byte[]>("test"));
        /*
         * The bug would only show itself if the second successful required
         * write was slow (but still within the timeout).
         */
        subStores.put(id2,
                      new SleepyStore<ByteArray, byte[]>(100,
                                                         new InMemoryStorageEngine<ByteArray, byte[]>("test")));

        setFailureDetectorVerifier(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        Store<ByteArray, byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, new Versioned<byte[]>(aValue));
    }

    @Test
    public void testPutTimeout() throws Exception {
        int timeout = 50;
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(3)
                                                                 .setRequiredReads(3)
                                                                 .setPreferredWrites(3)
                                                                 .setRequiredWrites(3)
                                                                 .build();
        Map<Integer, Store<ByteArray, byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        this.createFailureDetector();
        int totalDelay = 0;
        for(int i = 0; i < 3; i++) {
            int delay = 4 + i * timeout;
            totalDelay += delay;
            Store<ByteArray, byte[]> store = new SleepyStore<ByteArray, byte[]>(delay,
                                                                                new InMemoryStorageEngine<ByteArray, byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, 0, partitions));
        }

        setFailureDetectorVerifier(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       timeout);

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            true,
                                                            failureDetector);

        long start = System.currentTimeMillis();
        try {
            routedStore.put(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }));
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
        }
    }

    /**
     * See Issue #211: Unnecessary read repairs during getAll with more than one
     * key
     */
    @Test
    public void testNoReadRepair() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               2,
                                                               1,
                                                               3,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        this.createFailureDetector();
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();

        /* We just need to keep a store from one node */
        StatTrackingStore<ByteArray, byte[]> statTrackingStore = null;
        for(int i = 0; i < 3; ++i) {
            int id = Iterables.get(cluster.getNodes(), i).getId();
            statTrackingStore = new StatTrackingStore<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>("test"),
                                                                         null);
            subStores.put(id, statTrackingStore);

        }
        setFailureDetectorVerifier(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        ByteArray key1 = aKey;
        routedStore.put(key1, Versioned.value("value1".getBytes()));
        ByteArray key2 = TestUtils.toByteArray("voldemort");
        routedStore.put(key2, Versioned.value("value2".getBytes()));

        long putCount = statTrackingStore.getStats().getCount(Tracked.PUT);
        routedStore.getAll(Arrays.asList(key1, key2));
        /* Read repair happens asynchronously, so we wait a bit */
        Thread.sleep(500);
        assertEquals("put count should remain the same if there are no read repairs",
                     putCount,
                     statTrackingStore.getStats().getCount(Tracked.PUT));
    }

    @Test
    public void testTardyResponsesNotIncludedInResult() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               2,
                                                               3,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        int sleepTimeMs = 500;
        this.createFailureDetector();
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[]>(sleepTimeMs, store);
            }

            subStores.put(node.getId(), store);
        }

        setFailureDetectorVerifier(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       10000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        routedStore.put(aKey, Versioned.value(aValue));

        routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                    routedStoreThreadPool,
                                                    sleepTimeMs / 2);

        routedStore = routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);

        List<Versioned<byte[]>> versioneds = routedStore.get(aKey);
        assertEquals(2, versioneds.size());

        // Let's make sure that if the response *does* come in late, that it
        // doesn't alter the return value.
        Thread.sleep(sleepTimeMs * 2);
        assertEquals(2, versioneds.size());
    }

    @Test
    public void testSlowStoreDowngradesFromPreferredToRequired() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               2,
                                                               3,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        int sleepTimeMs = 500;
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        this.createFailureDetector();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[]>(sleepTimeMs, store);
            }

            subStores.put(node.getId(), store);
        }

        setFailureDetectorVerifier(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       10000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        routedStore.put(aKey, Versioned.value(aValue));

        routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                    routedStoreThreadPool,
                                                    sleepTimeMs / 2);

        routedStore = routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);

        List<Versioned<byte[]>> versioneds = routedStore.get(aKey);
        assertEquals(2, versioneds.size());
    }

    private void assertOperationalNodes(int expected) {
        int found = 0;
        for(Node n: cluster.getNodes())
            if(failureDetector.isAvailable(n))
                found++;
        assertEquals("Number of operational nodes not what was expected.", expected, found);
    }

    private void setFailureDetectorVerifier(Map<Integer, Store<ByteArray, byte[]>> subStores) {
        failureDetector.getConfig().setStoreVerifier(create(subStores));
    }

    private void createFailureDetector() {
        // Destroy any previous failure detector before creating the next one
        // (the final one is destroyed in tearDown).
        if(failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(2000)
                                                                                 .setNodes(cluster.getNodes());
        failureDetector = create(failureDetectorConfig, false);
    }

    @Test
    public void testInterruptedMasterPut() {
    /**
     * TODO final ByteArray key = TestUtils.toByteArray("key"); // Node 1 is
     * master
     * 
     * Cluster cluster = VoldemortTestConstants.getThreeNodeCluster();
     * StoreDefinition storeDef = ServerTestUtils.getStoreDef("test", 3, 3, 3,
     * 2, 2, RoutingStrategyType.CONSISTENT_STRATEGY); Map<Integer,
     * Store<ByteArray, byte[]>> memStores = Maps.newHashMap(); Map<Integer,
     * Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
     * 
     * memStores.put(0, new InMemoryStorageEngine<ByteArray, byte[]>("test-0"));
     * memStores.put(1, new InMemoryStorageEngine<ByteArray, byte[]>("test-1"));
     * memStores.put(2, new InMemoryStorageEngine<ByteArray, byte[]>("test-2"));
     * subStores.put(0, new SleepyStore<ByteArray, byte[]>(2000,
     * memStores.get(0))); subStores.put(1, new SleepyStore<ByteArray,
     * byte[]>(2000, memStores.get(1))); subStores.put(2, new
     * SleepyStore<ByteArray, byte[]>(2000, memStores.get(2)));
     * 
     * this.createFailureDetector(); final Store<ByteArray, byte[]> store = new
     * RoutedStore("test", subStores, cluster, storeDef, 2, true, 2500L,
     * TimeUnit.MILLISECONDS, failureDetector);
     * 
     * ExecutorService threadPool = Executors.newSingleThreadExecutor(); final
     * Version master = TestUtils.getClock(1, 1); Future<Version> child =
     * threadPool.submit(new Callable<Version>() {
     * 
     * public Version call() { Versioned<byte[]> value = new
     * Versioned<byte[]>(key.get(), master); Version version = store.put(key,
     * value); fail("Expected failure, not " + version); return version; } });
     * 
     * try { Thread.sleep(100); threadPool.shutdownNow(); Version version =
     * child.get(); for(Store<ByteArray, byte[]> mem: memStores.values()) {
     * List<Versioned<byte[]>> results = mem.get(key);
     * assertEquals("Only one version", 1, results.size());
     * assertEquals("Matching version", version, results.get(0).getVersion()); }
     * } catch(ExecutionException e) { if(e.getCause() != null) {
     * assertEquals("Unexpected exception",
     * InsufficientOperationalNodesException.class, e.getCause().getClass()); }
     * } catch(Exception e) { fail("Unexpected exception [" + e.getClass() +
     * "]: " + e.getMessage()); }
     */
    }

    @Test
    public void testInterruptedReplicaPut() {
    /**
     * **TODO final ByteArray key = TestUtils.toByteArray("key"); // Node 1 is
     * master
     * 
     * Cluster cluster = VoldemortTestConstants.getThreeNodeCluster();
     * StoreDefinition storeDef = ServerTestUtils.getStoreDef("test", 3, 3, 3,
     * 3, 3, RoutingStrategyType.CONSISTENT_STRATEGY); Map<Integer,
     * Store<ByteArray, byte[]>> memStores = Maps.newHashMap(); Map<Integer,
     * Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
     * 
     * memStores.put(0, new InMemoryStorageEngine<ByteArray, byte[]>("test-0"));
     * memStores.put(1, new InMemoryStorageEngine<ByteArray, byte[]>("test-1"));
     * memStores.put(2, new InMemoryStorageEngine<ByteArray, byte[]>("test-2"));
     * subStores.put(1, memStores.get(1)); subStores.put(0, new
     * SleepyStore<ByteArray, byte[]>(2000, memStores.get(0))); subStores.put(2,
     * new SleepyStore<ByteArray, byte[]>(2000, memStores.get(2)));
     * 
     * this.createFailureDetector(); final Store<ByteArray, byte[]> store = new
     * RoutedStore("test", subStores, cluster, storeDef, 2, true, 2500L,
     * TimeUnit.MILLISECONDS, failureDetector);
     * 
     * ExecutorService threadPool = Executors.newSingleThreadExecutor(); final
     * Version master = TestUtils.getClock(1, 1); Future<Version> child =
     * threadPool.submit(new Callable<Version>() {
     * 
     * public Version call() { try { Versioned<byte[]> value = new
     * Versioned<byte[]>(key.get(), master); Version version = store.put(key,
     * value); return version; } catch(Exception e) {
     * fail("Unexpected exception [" + e.getClass() + "]: " + e.getMessage());
     * return null; } } });
     * 
     * try { Thread.sleep(100); threadPool.shutdownNow(); Version version =
     * child.get(); for(Store<ByteArray, byte[]> mem: memStores.values()) {
     * List<Versioned<byte[]>> results = mem.get(key);
     * assertEquals("Only one version", 1, results.size());
     * assertEquals("Matching version", version, results.get(0).getVersion()); }
     * } catch(Exception e) { fail("Unexpected exception [" + e.getClass() +
     * "]: " + e.getMessage()); }
     */
    }
}
