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
import java.util.Collections;
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
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.ThreadedStore;
import voldemort.store.distributed.AbstractDistributedStoreTest;
import voldemort.store.distributed.DistributedParallelStore;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.failuredetector.FailureDetectingStore;
import voldemort.store.memory.InMemoryStore;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.Tracked;
import voldemort.store.versioned.VectorClockResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
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
    private final byte[] aTransform = "transform".getBytes();
    private final Class<? extends FailureDetector> failureDetectorClass;
    private final boolean isPipelineRoutedStoreEnabled;
    private FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;

    public RoutedStoreTest(Class<? extends FailureDetector> failureDetectorClass,
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
    public Store<ByteArray, byte[], byte[]> createStore(String name) {
        int nodes = cluster.getNumberOfNodes();
        return createStore(name, cluster, nodes, nodes, nodes, 0, 0);
    }

    public Store<ByteArray, byte[], byte[]> createStore(String name,
                                                        Cluster cluster,
                                                        int reads,
                                                        int writes,
                                                        int threads,
                                                        int failures,
                                                        int sleepy) {
        int nodes = cluster.getNumberOfNodes();
        StoreDefinition storeDef = this.getStoreDef(name,
                                                    nodes,
                                                    reads,
                                                    writes,
                                                    RoutingStrategyType.TO_ALL_STRATEGY);
        RoutedStore routed = createRoutedStore(cluster,
                                               storeDef,
                                               createDistributedStore(cluster,
                                                                      storeDef,
                                                                      threads,
                                                                      failures,
                                                                      sleepy,
                                                                      new VoldemortException("oops")));
        return new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routed);
    }

    private RoutedStore createRoutedStore(Cluster cluster,
                                          String name,
                                          int reads,
                                          int writes,
                                          String strategy,
                                          int threads,
                                          int failing,
                                          int sleepy,
                                          VoldemortException e) {
        return createRoutedStore(cluster,
                                 name,
                                 cluster.getNumberOfNodes(),
                                 reads,
                                 writes,
                                 strategy,
                                 threads,
                                 failing,
                                 sleepy,
                                 e);
    }

    public DistributedStore<Node, ByteArray, byte[], byte[]> createDistributedStore(Cluster cluster,
                                                                                    StoreDefinition storeDef,
                                                                                    int threads,
                                                                                    int failing,
                                                                                    int sleepy,
                                                                                    VoldemortException e) {
        createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> async = createClusterStores(cluster,
                                                                                            storeDef.getName(),
                                                                                            failing,
                                                                                            sleepy,
                                                                                            threads,
                                                                                            this.failureDetector,
                                                                                            e);
        return buildDistributedStore(async, storeDef);
    }

    private RoutedStore createRoutedStore(Cluster cluster,
                                          String name,
                                          int replicas,
                                          int reads,
                                          int writes,
                                          String strategy,
                                          int threads,
                                          int failing,
                                          int sleepy,
                                          VoldemortException e) {
        createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> async = createClusterStores(cluster,
                                                                                            name,
                                                                                            failing,
                                                                                            sleepy,
                                                                                            threads,
                                                                                            this.failureDetector,
                                                                                            e);
        StoreDefinition storeDef = getStoreDef(name, replicas, reads, writes, strategy);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = buildDistributedStore(async,
                                                                                              storeDef);
        setFailureDetectorVerifier(async);
        return createRoutedStore(cluster, storeDef, distributor);
    }

    public RoutedStore createRoutedStore(Cluster cluster,
                                         StoreDefinition storeDef,
                                         DistributedStore<Node, ByteArray, byte[], byte[]> distributor) {
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       1000L);
        return routedStoreFactory.create(cluster, storeDef, distributor);
    }

    private RoutedStore createRoutedStore(Cluster cluster,
                                          String name,
                                          int reads,
                                          int writes,
                                          int threads,
                                          int failing,
                                          int sleepy) {
        return createRoutedStore(cluster,
                                 name,
                                 reads,
                                 writes,
                                 RoutingStrategyType.TO_ALL_STRATEGY,
                                 threads,
                                 failing,
                                 sleepy,
                                 new VoldemortException());
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> createSleepyStore(Node node,
                                                                           String storeName,
                                                                           long delay,
                                                                           FailureDetector detector,
                                                                           ExecutorService threadPool) {
        Store<ByteArray, byte[], byte[]> memory = new InMemoryStore<ByteArray, byte[], byte[]>(storeName
                                                                                               + "_"
                                                                                               + node.getId());
        Store<ByteArray, byte[], byte[]> sleepy = new SleepyStore<ByteArray, byte[], byte[]>(delay,
                                                                                             memory);
        return createAsyncStore(node, sleepy, detector, threadPool);
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(Node node,
                                                                          Store<ByteArray, byte[], byte[]> store,
                                                                          FailureDetector detector,
                                                                          ExecutorService threadPool) {
        AsynchronousStore<ByteArray, byte[], byte[]> threaded = new ThreadedStore<ByteArray, byte[], byte[]>(AsyncUtils.asCallable(store),
                                                                                                             threadPool);
        return FailureDetectingStore.create(node, detector, threaded);
    }

    public static Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> createClusterStores(Cluster cluster,
                                                                                              String storeName,
                                                                                              int failing,
                                                                                              int sleepy,
                                                                                              int threads,
                                                                                              FailureDetector detector,
                                                                                              VoldemortException e) {
        if(failing + sleepy > cluster.getNumberOfNodes()) {
            throw new IllegalArgumentException(failing + " failing nodes, " + sleepy
                                               + " sleepy nodes, but only "
                                               + cluster.getNumberOfNodes()
                                               + " nodes in the cluster.");
        }
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> nodeStores = AbstractDistributedStoreTest.createClusterStores(cluster,
                                                                                                                              storeName,
                                                                                                                              threadPool,
                                                                                                                              detector,
                                                                                                                              sleepy,
                                                                                                                              Long.MAX_VALUE,
                                                                                                                              failing,
                                                                                                                              e);
        for(Map.Entry<Node, AsynchronousStore<ByteArray, byte[], byte[]>> entry: nodeStores.entrySet()) {
            Node node = entry.getKey();
            AsynchronousStore<ByteArray, byte[], byte[]> async = entry.getValue();
            nodeStores.put(node, FailureDetectingStore.create(node, detector, async));
        }
        return nodeStores;
    }

    private StoreDefinition getStoreDef(String name,
                                        int replicas,
                                        int reads,
                                        int writes,
                                        String strategy) {
        return ServerTestUtils.getStoreDef(name, replicas, reads, reads, writes, writes, strategy);
    }

    private int countOccurances(RoutedStore routedStore, ByteArray key, Versioned<byte[]> value) {
        int count = 0;
        for(AsynchronousStore<ByteArray, byte[], byte[]> store: routedStore.getNodeStores()
                                                                           .values())
            try {
                List<Versioned<byte[]>> results = store.submitGet(key, null).get();
                if(results.size() > 0 && Utils.deepEquals(results.get(0), value))
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
        RoutedStore routedStore = createRoutedStore(cluster,
                                                    storeName,
                                                    reads,
                                                    writes,
                                                    threads,
                                                    failures,
                                                    0);
        Store<ByteArray, byte[], byte[]> store = new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routedStore);
        Version clock = getClock(1);
        Versioned<byte[]> entry = new Versioned<byte[]>(aValue, clock);
        Version result = routedStore.put(aKey, entry, aTransform);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, result);
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);
        List<Versioned<byte[]>> found = store.get(aKey, aTransform);
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
        RoutedStore routedStore = createRoutedStore(cluster,
                                                    this.storeName,
                                                    cluster.getNumberOfNodes(),
                                                    reads,
                                                    writes,
                                                    RoutingStrategyType.TO_ALL_STRATEGY,
                                                    threads,
                                                    failures,
                                                    0,
                                                    new UnreachableStoreException("no go"));
        try {
            routedStore.put(aKey, versioned, aTransform);
            fail("Put succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            checkException(writes, availableForWrite, failures, e);
        }
        try {
            routedStore.get(aKey, aTransform);
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
    public void testObsoleteMasterFails() {
        // write me
    }

    @Test
    public void testOnlyNodeFailuresDisableNode() throws Exception {
        // test put
        cluster = getNineNodeCluster();

        RoutedStore store = createRoutedStore(cluster,
                                              this.storeName,
                                              1,
                                              cluster.getNumberOfNodes(),
                                              RoutingStrategyType.TO_ALL_STRATEGY,
                                              cluster.getNumberOfNodes(),
                                              cluster.getNumberOfNodes(),
                                              0,
                                              new VoldemortException());
        try {
            store.put(aKey, new Versioned<byte[]>(aValue), aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(cluster.getNumberOfNodes(),
                                cluster.getNumberOfNodes() - 1,
                                cluster.getNumberOfNodes(),
                                e);
        }
        assertOperationalNodes(cluster.getNumberOfNodes());

        cluster = getNineNodeCluster();

        store = createRoutedStore(cluster,
                                  this.storeName,
                                  cluster.getNumberOfNodes(),
                                  1,
                                  RoutingStrategyType.TO_ALL_STRATEGY,
                                  cluster.getNumberOfNodes(),
                                  cluster.getNumberOfNodes(),
                                  0,
                                  new UnreachableStoreException("no go"));
        try {
            store.put(aKey, new Versioned<byte[]>(aValue), aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 0, 9, e);
        }
        assertOperationalNodes(0);

        // test get
        cluster = getNineNodeCluster();

        store = createRoutedStore(cluster,
                                  storeName,
                                  1,
                                  cluster.getNumberOfNodes(),
                                  RoutingStrategyType.TO_ALL_STRATEGY,
                                  cluster.getNumberOfNodes(),
                                  9,
                                  0,
                                  new VoldemortException());
        try {
            store.get(aKey, aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 9, 9, e);
        }
        assertOperationalNodes(cluster.getNumberOfNodes());

        cluster = getNineNodeCluster();

        store = createRoutedStore(cluster,
                                  storeName,
                                  1,
                                  cluster.getNumberOfNodes(),
                                  RoutingStrategyType.TO_ALL_STRATEGY,
                                  cluster.getNumberOfNodes(),
                                  cluster.getNumberOfNodes(),
                                  0,
                                  new UnreachableStoreException("no go"));
        try {
            store.get(aKey, aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), e);
        }
        assertOperationalNodes(0);

        // test delete
        cluster = getNineNodeCluster();

        store = createRoutedStore(cluster,
                                  storeName,
                                  1,
                                  cluster.getNumberOfNodes(),
                                  RoutingStrategyType.TO_ALL_STRATEGY,
                                  cluster.getNumberOfNodes(),
                                  cluster.getNumberOfNodes(),
                                  0,
                                  new VoldemortException());
        try {
            store.delete(aKey, new VectorClock());
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(9, 9, 9, e);
        }
        assertOperationalNodes(cluster.getNumberOfNodes());

        cluster = getNineNodeCluster();

        store = createRoutedStore(cluster,
                                  storeName,
                                  1,
                                  1,
                                  RoutingStrategyType.TO_ALL_STRATEGY,
                                  cluster.getNumberOfNodes(),
                                  cluster.getNumberOfNodes(),
                                  0,
                                  new UnreachableStoreException("no go"));
        try {
            store.delete(aKey, new VectorClock());
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), e);
        }
        assertOperationalNodes(0);
    }

    @Test
    public void testGetVersions2() throws Exception {
        List<ByteArray> keys = getKeys(2);
        ByteArray key = keys.get(0);
        byte[] value = getValue();
        Store<ByteArray, byte[], byte[]> store = getStore();
        store.put(key, Versioned.value(value), null);
        List<Versioned<byte[]>> versioneds = store.get(key, null);
        List<Version> versions = store.getVersions(key);
        assertEquals(1, versioneds.size());
        assertEquals(1, versions.size());
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

        createFailureDetector();
        RoutedStore routedStore = createRoutedStore(cluster, storeName, 1, 2, 1, 0, 0);
        Store<ByteArray, byte[], byte[]> store = new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routedStore);

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }

        recordException(failureDetector, cluster.getNodes().iterator().next());

        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet(), null);
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

        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        this.createFailureDetector();

        Node node1 = Iterables.get(cluster.getNodes(), 0);
        Node node2 = Iterables.get(cluster.getNodes(), 1);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        subStores.put(node1,
                      createAsyncStore(node1,
                                       new InMemoryStore<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       threadPool));
        subStores.put(node2,
                      createAsyncStore(node2,
                                       new FailingReadsStore<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       threadPool));

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            buildDistributedStore(subStores,
                                                                                  storeDef));

        Store<ByteArray, byte[], byte[]> store = new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routedStore);

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }
        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet(), null);
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

        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        this.createFailureDetector();

        routedStoreThreadPool = Executors.newFixedThreadPool(1);

        Node node1 = Iterables.get(cluster.getNodes(), 0);
        Node node2 = Iterables.get(cluster.getNodes(), 1);
        subStores.put(node1,
                      createAsyncStore(node1,
                                       new InMemoryStore<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       routedStoreThreadPool));
        subStores.put(node2,
                      createAsyncStore(node2,
                                       new InMemoryStore<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       routedStoreThreadPool));

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            buildDistributedStore(subStores,
                                                                                  storeDef));

        Store<ByteArray, byte[], byte[]> store = new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routedStore);
        this.setFailureDetectorVerifier(subStores);
        store.put(aKey, Versioned.value(aValue), aTransform);
        recordException(failureDetector, cluster.getNodes().iterator().next());
        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(Arrays.asList(aKey),
                                                                   Collections.singletonMap(aKey,
                                                                                            aTransform));
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

        RoutedStore routedStore = createRoutedStore(cluster,
                                                    storeName,
                                                    cluster.getNumberOfNodes() - 1,
                                                    cluster.getNumberOfNodes() - 1,
                                                    1,
                                                    0,
                                                    0);
        // Disable node 1 so that the first put also goes to the last node
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 1));
        Store<ByteArray, byte[], byte[]> store = new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routedStore);
        store.put(aKey, new Versioned<byte[]>(aValue), null);

        byte[] anotherValue = "john".getBytes();

        // Disable the last node and enable node 1 to prevent the last node from
        // getting the new version
        recordException(failureDetector, Iterables.getLast(cluster.getNodes()));
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 1));
        Version clock = getClock(1);
        store.put(aKey, new Versioned<byte[]>(anotherValue, clock), null);

        // Enable last node and disable node 1, the following get should cause a
        // read repair on the last node in the code path that is only executed
        // if there are failures.
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 1));
        recordSuccess(failureDetector, Iterables.getLast(cluster.getNodes()));
        List<Versioned<byte[]>> versioneds = store.get(aKey, null);
        assertEquals(1, versioneds.size());
        assertEquals(new ByteArray(anotherValue), new ByteArray(versioneds.get(0).getValue()));

        // Read repairs are done asynchronously, so we sleep for a short period.
        // It may be a good idea to use a synchronous executor service.
        Thread.sleep(100);
        for(Map.Entry<Node, AsynchronousStore<ByteArray, byte[], byte[]>> entry: routedStore.getNodeStores()
                                                                                            .entrySet()) {
            recordSuccess(failureDetector, entry.getKey());
            AsynchronousStore<ByteArray, byte[], byte[]> inner = entry.getValue();
            List<Versioned<byte[]>> innerVersioneds = inner.submitGet(aKey, null).get();
            assertEquals(1, versioneds.size());
            assertEquals(new ByteArray(anotherValue), new ByteArray(innerVersioneds.get(0)
                                                                                   .getValue()));
        }

    }

    protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                      StoreDefinition storeDef) {
        return buildDistributedStore(stores, storeDef, true);
    }

    protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                      StoreDefinition storeDef,
                                                                                      boolean makeUnique) {
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = new DistributedParallelStore<Node, ByteArray, byte[], byte[]>(stores,
                                                                                                                                      storeDef,
                                                                                                                                      makeUnique);
        return new ReadRepairStore<Node, ByteArray, byte[], byte[]>(distributor);
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
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        Node node1 = Iterables.get(cluster.getNodes(), 0);
        Node node2 = Iterables.get(cluster.getNodes(), 1);
        Node node3 = Iterables.get(cluster.getNodes(), 2);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);

        Store<ByteArray, byte[], byte[]> failing = FailingStore.asStore("test");
        subStores.put(node1,
                      createAsyncStore(node1, failing, failureDetector, routedStoreThreadPool));
        subStores.put(node3,
                      createAsyncStore(node3,
                                       new InMemoryStore<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       routedStoreThreadPool));
        /*
         * The bug would only show itself if the second successful required
         * write was slow (but still within the timeout).
         */
        subStores.put(node2,
                      createSleepyStore(node2, "test", 100, failureDetector, routedStoreThreadPool));

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            buildDistributedStore(subStores,
                                                                                  storeDef));

        Store<ByteArray, byte[], byte[]> store = new VectorClockResolvingStore<ByteArray, byte[], byte[]>(routedStore);
        store.put(aKey, new Versioned<byte[]>(aValue), aTransform);
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
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = new HashMap<Node, AsynchronousStore<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        this.createFailureDetector();
        routedStoreThreadPool = Executors.newFixedThreadPool(3);
        int totalDelay = 0;
        for(int i = 0; i < 3; i++) {
            List<Integer> partitions = Arrays.asList(i);
            Node node = new Node(i, "none", 0, 0, 0, partitions);
            int delay = 4 + i * timeout;
            totalDelay += delay;
            stores.put(node, this.createSleepyStore(node,
                                                    "test",
                                                    delay,
                                                    failureDetector,
                                                    routedStoreThreadPool));
            nodes.add(node);
        }

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       timeout);
        Cluster cluster = new Cluster("test", nodes);
        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            definition,
                                                            buildDistributedStore(stores,
                                                                                  definition));

        long start = System.currentTimeMillis();
        try {
            routedStore.put(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }),
                            null);
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
        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        /* We just need to keep a store from one node */
        StatTrackingStore<ByteArray, byte[], byte[]> statStore = null;
        for(int i = 0; i < 3; ++i) {
            Node node = Iterables.get(cluster.getNodes(), i);
            statStore = new StatTrackingStore<ByteArray, byte[], byte[]>(new InMemoryStore<ByteArray, byte[], byte[]>("test"),
                                                                         null);
            AsynchronousStore<ByteArray, byte[], byte[]> async = this.createAsyncStore(node,
                                                                                       statStore,
                                                                                       failureDetector,
                                                                                       routedStoreThreadPool);
            subStores.put(node, async);
        }

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            buildDistributedStore(subStores,
                                                                                  storeDef,
                                                                                  false));

        ByteArray key1 = aKey;
        routedStore.put(key1, Versioned.value("value1".getBytes()), null);
        ByteArray key2 = TestUtils.toByteArray("voldemort");
        routedStore.put(key2, Versioned.value("value2".getBytes()), null);

        long putCount = statStore.getStats().getCount(Tracked.PUT);
        routedStore.getAll(Arrays.asList(key1, key2), null);
        /* Read repair happens asynchronously, so we wait a bit */
        Thread.sleep(500);
        assertEquals("put count should remain the same if there are no read repairs",
                     putCount,
                     statStore.getStats().getCount(Tracked.PUT));
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
        routedStoreThreadPool = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> store = new InMemoryStore<ByteArray, byte[], byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[], byte[]>(sleepTimeMs, store);
            }

            subStores.put(node,
                          this.createAsyncStore(node, store, failureDetector, routedStoreThreadPool));
        }

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       10000L);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = buildDistributedStore(subStores,
                                                                                              storeDef,
                                                                                              false);

        RoutedStore routedStore = routedStoreFactory.create(cluster, storeDef, distributor);

        routedStore.put(aKey, Versioned.value(aValue), null);

        routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                    failureDetector,
                                                    sleepTimeMs / 2);

        routedStore = routedStoreFactory.create(cluster, storeDef, distributor);

        List<Versioned<byte[]>> versioneds = routedStore.get(aKey, null);
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
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        this.createFailureDetector();
        routedStoreThreadPool = Executors.newFixedThreadPool(cluster.getNumberOfNodes());

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> store = new InMemoryStore<ByteArray, byte[], byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[], byte[]>(sleepTimeMs, store);
            }

            subStores.put(node,
                          this.createAsyncStore(node, store, failureDetector, routedStoreThreadPool));
        }

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       failureDetector,
                                                                       10000L);

        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = buildDistributedStore(subStores,
                                                                                              storeDef,
                                                                                              false);
        RoutedStore routedStore = routedStoreFactory.create(cluster, storeDef, distributor);

        routedStore.put(aKey, Versioned.value(aValue), null);

        routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                    failureDetector,
                                                    sleepTimeMs / 2);

        routedStore = routedStoreFactory.create(cluster, storeDef, distributor);

        List<Versioned<byte[]>> versioneds = routedStore.get(aKey, null);
        assertEquals(2, versioneds.size());
    }

    private void assertOperationalNodes(int expected) {
        int found = 0;
        for(Node n: cluster.getNodes())
            if(failureDetector.isAvailable(n)) {
                found++;
            }
        assertEquals("Number of operational nodes not what was expected.", expected, found);
    }

    private void setFailureDetectorVerifier(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> asyncStores) {
        Map<Integer, Store<ByteArray, byte[], byte[]>> syncStores = Maps.newHashMap();
        for(Map.Entry<Node, AsynchronousStore<ByteArray, byte[], byte[]>> entry: asyncStores.entrySet()) {
            syncStores.put(entry.getKey().getId(), AsyncUtils.asSync(entry.getValue()));
        }
        failureDetector.getConfig().setStoreVerifier(create(syncStores));
    }

    public static FailureDetector createFailureDetector(Class<? extends FailureDetector> failureDetectorClass,
                                                        Cluster cluster) {
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(2000)
                                                                                 .setNodes(cluster.getNodes());
        return create(failureDetectorConfig, false);
    }

    public FailureDetector createFailureDetector() {
        // Destroy any previous failure detector before creating the next one
        // (the final one is destroyed in tearDown).
        if(failureDetector != null)
            failureDetector.destroy();
        failureDetector = createFailureDetector(this.failureDetectorClass, this.cluster);
        return failureDetector;
    }
}
