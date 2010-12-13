package voldemort.store.distributed;

import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.MutableStoreVerifier.create;
import static voldemort.TestUtils.getClock;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import voldemort.store.FailingReadsStore;
import voldemort.store.FailingStore;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.AbstractAsynchronousStoreTest;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.CallableStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.ThreadedStore;
import voldemort.store.failuredetector.FailureDetectingStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

abstract public class AbstractDistributedStoreTest extends AbstractAsynchronousStoreTest {

    protected Cluster cluster;
    private final ByteArray aKey = TestUtils.toByteArray("jay");
    private final byte[] aValue = "kreps".getBytes();
    private final byte[] aTransform = "transform".getBytes();
    protected FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;
    private Map<String, DistributedStore<Node, ByteArray, byte[], byte[]>> distributors;
    private Map<String, StoreDefinition> definitions;

    public AbstractDistributedStoreTest(String name) {
        super(name);
        distributors = new HashMap<String, DistributedStore<Node, ByteArray, byte[], byte[]>>();
        definitions = new HashMap<String, StoreDefinition>();
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

    protected StoreDefinition createStoreDef(String name) {
        return getStoreDef(name,
                           cluster.getNumberOfNodes(),
                           cluster.getNumberOfNodes(),
                           cluster.getNumberOfNodes(),
                           RoutingStrategyType.TO_ALL_STRATEGY);
    }

    protected StoreDefinition getStoreDef(String name) {
        StoreDefinition storeDef = definitions.get(name);
        if(storeDef == null) {
            storeDef = createStoreDef(name);
            definitions.put(name, storeDef);
        }
        return storeDef;
    }

    protected DistributedStore<Node, ByteArray, byte[], byte[]> getDistributedStore(StoreDefinition storeDef) {
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = distributors.get(storeDef);
        if(distributor == null) {
            distributor = createDistributedStore(storeDef);
            distributors.put(storeDef.getName(), distributor);
        }
        return distributor;
    }

    protected DistributedStore<Node, ByteArray, byte[], byte[]> createDistributedStore(StoreDefinition storeDef) {
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             storeName,
                                                                                             cluster.getNumberOfNodes(),
                                                                                             failureDetector,
                                                                                             0,
                                                                                             0,
                                                                                             0,
                                                                                             null);
        return this.buildDistributedStore(stores, cluster, storeDef);

    }

    @Override
    public AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(String name) {
        StoreDefinition storeDef = getStoreDef(name);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = getDistributedStore(storeDef);
        return DistributedStoreFactory.asAsync(storeDef, distributor);
    }

    @Override
    protected AsynchronousStore<ByteArray, byte[], byte[]> createSlowStore(String name, long delay) {
        StoreDefinition storeDef = getStoreDef(name);
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             storeName,
                                                                                             cluster.getNumberOfNodes(),
                                                                                             failureDetector,
                                                                                             cluster.getNumberOfNodes(),
                                                                                             delay,
                                                                                             0,
                                                                                             null);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = this.buildDistributedStore(stores,
                                                                                                   cluster,
                                                                                                   storeDef);
        return DistributedStoreFactory.asAsync(storeDef, distributor);
    }

    @Override
    protected AsynchronousStore<ByteArray, byte[], byte[]> createFailingStore(String name,
                                                                              VoldemortException ex) {
        StoreDefinition storeDef = getStoreDef(name);
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             storeName,
                                                                                             cluster.getNumberOfNodes(),
                                                                                             failureDetector,
                                                                                             0,
                                                                                             0,
                                                                                             cluster.getNumberOfNodes(),
                                                                                             ex);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = this.buildDistributedStore(stores,
                                                                                                   cluster,
                                                                                                   storeDef);
        return DistributedStoreFactory.asAsync(storeDef, distributor);
    }

    private int countOccurances(DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                                ByteArray key,
                                Versioned<byte[]> value) {
        int count = 0;
        for(AsynchronousStore<ByteArray, byte[], byte[]> store: distributor.getNodeStores()
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

    private void assertNEqual(DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                              int expected,
                              ByteArray key,
                              Versioned<byte[]> value) {
        int count = countOccurances(distributor, key, value);
        assertEquals("Expected " + expected + " occurances of '" + key + "' with value '" + value
                     + "', but found " + count + ".", expected, count);
    }

    private void assertNOrMoreEqual(DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                                    int expected,
                                    ByteArray key,
                                    Versioned<byte[]> value) {
        int count = countOccurances(distributor, key, value);
        assertTrue("Expected " + expected + " or more occurances of '" + key + "' with value '"
                   + value + "', but found " + count + ".", expected <= count);
    }

    protected StoreDefinition getStoreDef(String name, int reads, int writes) {
        return getStoreDef(name,
                           cluster.getNumberOfNodes(),
                           reads,
                           writes,
                           RoutingStrategyType.TO_ALL_STRATEGY);
    }

    protected StoreDefinition getStoreDef(String name, int replicas, int reads, int writes) {
        return getStoreDef(name, replicas, reads, writes, RoutingStrategyType.TO_ALL_STRATEGY);
    }

    protected StoreDefinition getStoreDef(String name,
                                          int replicas,
                                          int reads,
                                          int writes,
                                          String strategy) {
        return ServerTestUtils.getStoreDef(name, replicas, reads, reads, writes, writes, strategy);
    }

    protected Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> createClusterStores(Cluster cluster,
                                                                                          String storeName,
                                                                                          int threads,
                                                                                          FailureDetector detector,
                                                                                          int sleepy,
                                                                                          long delay,
                                                                                          int failing,
                                                                                          VoldemortException e) {
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        return createClusterStores(cluster,
                                   storeName,
                                   threadPool,
                                   detector,
                                   sleepy,
                                   delay,
                                   failing,
                                   e);
    }

    public static AsynchronousStore<ByteArray, byte[], byte[]> buildFailingStore(String name,
                                                                                 VoldemortException ex,
                                                                                 ExecutorService threadPool) {
        CallableStore<ByteArray, byte[], byte[]> store = new FailingStore<ByteArray, byte[], byte[]>(name,
                                                                                                     ex);
        return new ThreadedStore<ByteArray, byte[], byte[]>(store, threadPool);
    }

    public static AsynchronousStore<ByteArray, byte[], byte[]> buildMemoryStore(String name,
                                                                                long timeout,
                                                                                ExecutorService threadPool) {
        Store<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(name);
        if(timeout > 0) {
            store = new SleepyStore<ByteArray, byte[], byte[]>(timeout, store);
        }
        return new ThreadedStore<ByteArray, byte[], byte[]>(store, threadPool);
    }

    public static Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> createClusterStores(Cluster cluster,
                                                                                              String name,
                                                                                              ExecutorService threadPool,
                                                                                              FailureDetector detector,
                                                                                              int sleepy,
                                                                                              long delay,
                                                                                              int failing,
                                                                                              VoldemortException ex) {

        if(failing + sleepy > cluster.getNumberOfNodes()) {
            throw new IllegalArgumentException(failing + " failing nodes, " + sleepy
                                               + " sleepy nodes, but only "
                                               + cluster.getNumberOfNodes()
                                               + " nodes in the cluster.");
        }
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = Maps.newHashMap();
        int i = 0;
        for(Node node: cluster.getNodes()) {
            String storeName = name + "_" + i;
            AsynchronousStore<ByteArray, byte[], byte[]> async;
            if(i < failing) {
                async = buildFailingStore("failed(" + storeName + ")", ex, threadPool);
            } else if(i < failing + sleepy) {
                async = buildMemoryStore("Sleepy (" + storeName + ")", delay, threadPool);
            } else {
                async = buildMemoryStore("Good (" + storeName + ")", 0, threadPool);
            }
            if(detector != null) {
                async = FailureDetectingStore.create(node, detector, async);
            }
            i++;
            stores.put(node, async);
        }
        return stores;
    }

    private DistributedStore<Node, ByteArray, byte[], byte[]> createDistributedStore(Cluster cluster,
                                                                                     StoreDefinition storeDef,
                                                                                     int threads,
                                                                                     int sleepy,
                                                                                     long delay,
                                                                                     int failing,
                                                                                     VoldemortException e) {
        createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> async = createClusterStores(cluster,
                                                                                            storeDef.getName(),
                                                                                            threads,
                                                                                            this.failureDetector,
                                                                                            sleepy,
                                                                                            delay,
                                                                                            failing,
                                                                                            e);
        setFailureDetectorVerifier(async);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = buildDistributedStore(async,
                                                                                              cluster,
                                                                                              storeDef);
        return distributor;
    }

    protected <R> R waitForCompletion(StoreFuture<R> future) throws VoldemortException {
        return future.get();
    }

    private void testBasicOperations(int reads, int writes, int failures, int threads)
            throws Exception {
        StoreDefinition storeDef = getStoreDef(storeName, reads, writes);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = createDistributedStore(cluster,
                                                                                               storeDef,
                                                                                               threads,
                                                                                               0,
                                                                                               0,
                                                                                               failures,
                                                                                               new VoldemortException("oops"));
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             distributor);
        Version clock = getClock(1);
        Versioned<byte[]> entry = new Versioned<byte[]>(aValue, clock);
        Version result = waitForCompletion(async.submitPut(aKey, entry, aTransform));
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, result);
        assertNOrMoreEqual(distributor, cluster.getNumberOfNodes() - failures, aKey, versioned);
        List<Versioned<byte[]>> found = waitForCompletion(async.submitGet(aKey, aTransform));
        assertEquals(1, found.size());
        assertEquals(versioned, found.get(0));
        assertNOrMoreEqual(distributor, cluster.getNumberOfNodes() - failures, aKey, versioned);
        assertTrue(waitForCompletion(async.submitDelete(aKey, versioned.getVersion())));
        assertNEqual(distributor, 0, aKey, versioned);
        assertTrue(!(waitForCompletion(async.submitDelete(aKey, versioned.getVersion()))));
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
            // assertEquals("Successful Count", total - failures,
            // isne.getSuccessful());
        }
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(String storeName,
                                                                          int reads,
                                                                          int writes,
                                                                          int threads,
                                                                          int sleepy,
                                                                          long delay,
                                                                          int failures,
                                                                          VoldemortException ex) {

        StoreDefinition storeDef = getStoreDef(storeName, reads, writes);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = createDistributedStore(cluster,
                                                                                               storeDef,
                                                                                               threads,
                                                                                               sleepy,
                                                                                               delay,
                                                                                               failures,
                                                                                               ex);
        return DistributedStoreFactory.asAsync(storeDef, distributor);
    }

    protected AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(StoreDefinition storeDef,
                                                                            Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> asyncs) {
        return createAsyncStore(cluster, storeDef, asyncs);
    }

    protected AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(Cluster cluster,
                                                                            StoreDefinition storeDef,
                                                                            Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> asyncs) {
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = buildDistributedStore(asyncs,
                                                                                              cluster,
                                                                                              storeDef);
        return DistributedStoreFactory.asAsync(storeDef, distributor);
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(String storeName,
                                                                          int reads,
                                                                          int writes,
                                                                          int threads) {
        return createAsyncStore(storeName, reads, writes, threads, 0, 0, 0, null);
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(Node node,
                                                                          Store<ByteArray, byte[], byte[]> store,
                                                                          FailureDetector detector,
                                                                          ExecutorService threadPool) {
        AsynchronousStore<ByteArray, byte[], byte[]> threaded = new ThreadedStore<ByteArray, byte[], byte[]>(AsyncUtils.asCallable(store),
                                                                                                             threadPool);
        return FailureDetectingStore.create(node, detector, threaded);
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> createSleepyStore(Node node,
                                                                           String storeName,
                                                                           long delay,
                                                                           FailureDetector detector,
                                                                           ExecutorService threadPool) {
        Store<ByteArray, byte[], byte[]> memory = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName
                                                                                                       + "_"
                                                                                                       + node.getId());
        Store<ByteArray, byte[], byte[]> sleepy = new SleepyStore<ByteArray, byte[], byte[]>(delay,
                                                                                             memory);
        return createAsyncStore(node, sleepy, detector, threadPool);
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
                    version = super.checkForObsoleteVersion(clock,
                                                            count,
                                                            key,
                                                            versioned,
                                                            (VoldemortException) cause);
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

    private void testBasicOperationFailure(int reads, int writes, int failures, int threads)
            throws Exception {
        Version clock = getClock(1);
        int availableForWrite = cluster.getNumberOfNodes(); // Math.max(cluster.getNumberOfNodes()
        // - failures,
        // writes - 1);
        int availableForRead = cluster.getNumberOfNodes(); // Math.max(cluster.getNumberOfNodes()
        // - failures, reads
        // - 1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);
        StoreDefinition storeDef = getStoreDef(storeName, reads, writes);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = createDistributedStore(cluster,
                                                                                               storeDef,
                                                                                               threads,
                                                                                               0,
                                                                                               0,
                                                                                               failures,
                                                                                               new UnreachableStoreException("no go"));
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             distributor);
        try {
            waitForCompletion(async.submitPut(aKey, versioned, aTransform));
            fail("Put succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            checkException(writes, availableForWrite, failures, e);
        }
        try {
            waitForCompletion(async.submitGet(aKey, aTransform));
            fail("Get succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            checkException(reads, availableForRead, failures, e);
        }
        try {
            waitForCompletion(async.submitDelete(aKey, versioned.getVersion()));
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
    public void testOnlyNodeFailuresDisableNode() throws Exception {
        // test put
        cluster = getNineNodeCluster();

        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(storeName,
                                                                              1,
                                                                              cluster.getNumberOfNodes(),
                                                                              cluster.getNumberOfNodes(),
                                                                              0,
                                                                              0,
                                                                              cluster.getNumberOfNodes(),
                                                                              new VoldemortException("oops"));
        try {
            waitForCompletion(async.submitPut(aKey, new Versioned<byte[]>(aValue), aTransform));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(cluster.getNumberOfNodes(),
                                cluster.getNumberOfNodes(),
                                cluster.getNumberOfNodes(),
                                e);
        }
        assertOperationalNodes(cluster.getNumberOfNodes());

        async = createAsyncStore(storeName,
                                 cluster.getNumberOfNodes(),
                                 1,
                                 cluster.getNumberOfNodes(),
                                 0,
                                 0,
                                 cluster.getNumberOfNodes(),
                                 new UnreachableStoreException("no go"));
        try {
            waitForCompletion(async.submitPut(aKey, new Versioned<byte[]>(aValue), aTransform));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), e);
        }
        assertOperationalNodes(0);

        // test get
        cluster = getNineNodeCluster();

        async = createAsyncStore(storeName,
                                 1,
                                 cluster.getNumberOfNodes(),
                                 cluster.getNumberOfNodes(),
                                 0,
                                 0,
                                 cluster.getNumberOfNodes(),
                                 new VoldemortException());
        try {
            waitForCompletion(async.submitGet(aKey, aTransform));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, 9, 9, e);
        }
        assertOperationalNodes(cluster.getNumberOfNodes());
        async = createAsyncStore(storeName,
                                 1,
                                 cluster.getNumberOfNodes(),
                                 cluster.getNumberOfNodes(),
                                 0,
                                 0,
                                 cluster.getNumberOfNodes(),
                                 new UnreachableStoreException("no go"));
        try {
            waitForCompletion(async.submitGet(aKey, aTransform));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(1, cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), e);
        }
        assertOperationalNodes(0);

        // test delete

        async = createAsyncStore(storeName,
                                 1,
                                 cluster.getNumberOfNodes(),
                                 cluster.getNumberOfNodes(),
                                 0,
                                 0,
                                 cluster.getNumberOfNodes(),
                                 new VoldemortException());
        try {
            waitForCompletion(async.submitDelete(aKey, new VectorClock()));
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
            this.checkException(9, 9, 9, e);
        }
        assertOperationalNodes(cluster.getNumberOfNodes());

        cluster = getNineNodeCluster();

        async = createAsyncStore(storeName,
                                 1,
                                 1,
                                 cluster.getNumberOfNodes(),
                                 0,
                                 0,
                                 cluster.getNumberOfNodes(),
                                 new UnreachableStoreException("no go"));
        try {
            waitForCompletion(async.submitDelete(aKey, new VectorClock()));
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
        doPut(key, value);
        List<Versioned<byte[]>> versioneds = this.doGet(key);
        List<Version> versions = doGetVersions(key);
        assertEquals(1, versioneds.size());
        assertEquals(1, versions.size());
        for(int i = 0; i < versions.size(); i++)
            assertEquals(versioneds.get(0).getVersion(), versions.get(i));

        assertEquals(0, doGetVersions(keys.get(1)).size());
    }

    /**
     * Tests that getAll works correctly with a node down in a two node cluster.
     */
    @Test
    public void testGetAllWithNodeDown() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        createFailureDetector();
        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(storeName, 1, 2, 1);
        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            waitForCompletion(async.submitPut(key, Versioned.value(value), null));
            expectedValues.put(key, value);
        }

        recordException(failureDetector, cluster.getNodes().iterator().next());

        Map<ByteArray, List<Versioned<byte[]>>> all = waitForCompletion(async.submitGetAll(expectedValues.keySet(),
                                                                                           null));
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
                                       new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       threadPool));
        subStores.put(node2,
                      createAsyncStore(node2,
                                       new FailingReadsStore<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       threadPool));

        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(storeDef, subStores);

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            waitForCompletion(async.submitPut(key, Versioned.value(value), null));
            expectedValues.put(key, value);
        }
        Map<ByteArray, List<Versioned<byte[]>>> all = waitForCompletion(async.submitGetAll(expectedValues.keySet(),
                                                                                           null));
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
                                       new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       routedStoreThreadPool));
        subStores.put(node2,
                      createAsyncStore(node2,
                                       new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       routedStoreThreadPool));

        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(storeDef, subStores);

        this.setFailureDetectorVerifier(subStores);
        waitForCompletion(async.submitPut(aKey, Versioned.value(aValue), aTransform));
        recordException(failureDetector, cluster.getNodes().iterator().next());
        Map<ByteArray, List<Versioned<byte[]>>> all = waitForCompletion(async.submitGetAll(Arrays.asList(aKey),
                                                                                           Collections.singletonMap(aKey,
                                                                                                                    aTransform)));
        assertEquals(1, all.size());
        assertTrue(Arrays.equals(aValue, all.values().iterator().next().get(0).getValue()));
    }

    protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                      Cluster cluster,
                                                                                      StoreDefinition storeDef) {
        return buildDistributedStore(stores, cluster, storeDef, true);
    }

    abstract protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                               Cluster cluster,
                                                                                               StoreDefinition storeDef,
                                                                                               boolean makeUnique);

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
        subStores.put(node1, createAsyncStore(node1,
                                              failing,
                                              failureDetector,
                                              routedStoreThreadPool));
        subStores.put(node3,
                      createAsyncStore(node3,
                                       new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                       failureDetector,
                                       routedStoreThreadPool));
        /*
         * The bug would only show itself if the second successful required
         * write was slow (but still within the timeout).
         */
        subStores.put(node2, createSleepyStore(node2,
                                               "test",
                                               100,
                                               failureDetector,
                                               routedStoreThreadPool));

        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(storeDef, subStores);

        waitForCompletion(async.submitPut(aKey, new Versioned<byte[]>(aValue), aTransform));
    }

    public static void testIllegalArguments(DistributedStore<Node, ByteArray, byte[], byte[]> store,
                                            String reason,
                                            ByteArray key,
                                            byte[] value,
                                            List<Node> nodes,
                                            int preferred,
                                            int required,
                                            Class<? extends Exception> expected) {
        try {
            store.submitGet(key, null, nodes, preferred, required);
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
            store.submitPut(key, new Versioned<byte[]>(value), null, nodes, preferred, required);
            fail("Put should fail: " + reason);
        } catch(Exception e) {
            assertEquals("Expected exception: " + reason, expected, e.getClass());
        }
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

        Cluster cluster = new Cluster("test", nodes);
        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(cluster,
                                                                              definition,
                                                                              stores);

        long start = System.currentTimeMillis();
        try {
            async.submitPut(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }),
                            null).get(timeout, TimeUnit.MILLISECONDS);
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
            statStore = new StatTrackingStore<ByteArray, byte[], byte[]>(new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                                                         null);
            AsynchronousStore<ByteArray, byte[], byte[]> async = this.createAsyncStore(node,
                                                                                       statStore,
                                                                                       failureDetector,
                                                                                       routedStoreThreadPool);
            subStores.put(node, async);
        }

        AsynchronousStore<ByteArray, byte[], byte[]> async = createAsyncStore(cluster,
                                                                              storeDef,
                                                                              subStores);
        ByteArray key1 = aKey;
        waitForCompletion(async.submitPut(key1, Versioned.value("value1".getBytes()), null));
        ByteArray key2 = TestUtils.toByteArray("voldemort");
        waitForCompletion(async.submitPut(key2, Versioned.value("value2".getBytes()), null));

        long putCount = statStore.getStats().getCount(Tracked.PUT);
        waitForCompletion(async.submitGetAll(Arrays.asList(key1, key2), null));
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
            Store<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[], byte[]>(sleepTimeMs, store);
            }

            subStores.put(node, this.createAsyncStore(node,
                                                      store,
                                                      failureDetector,
                                                      routedStoreThreadPool));
        }

        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = this.buildDistributedStore(subStores,
                                                                                                   cluster,
                                                                                                   storeDef,
                                                                                                   false);
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             distributor);

        async.submitPut(aKey, Versioned.value(aValue), null).get(10000L, TimeUnit.MILLISECONDS);

        List<Versioned<byte[]>> versioneds = async.submitGet(aKey, null).get(sleepTimeMs / 2,
                                                                             TimeUnit.MILLISECONDS);
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
            Store<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[], byte[]>(sleepTimeMs, store);
            }

            subStores.put(node, this.createAsyncStore(node,
                                                      store,
                                                      failureDetector,
                                                      routedStoreThreadPool));
        }

        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = buildDistributedStore(subStores,
                                                                                              cluster,
                                                                                              storeDef,
                                                                                              false);
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             distributor);

        async.submitPut(aKey, Versioned.value(aValue), null).get(10000L, TimeUnit.MILLISECONDS);
        List<Versioned<byte[]>> versioneds = async.submitGet(aKey, null).get(sleepTimeMs / 2,
                                                                             TimeUnit.MILLISECONDS);
        assertEquals(2, versioneds.size());
    }

    @Test
    public void testFailingStores() {
        // Test that the request fails quickly.
        // Build a store with 1 good, 4 bad, and 1 sleepy, require/prefer 4
        // responses.
        int required = 5;
        int failures = 5;
        int available = cluster.getNumberOfNodes();
        StoreDefinition storeDef = this.getStoreDef("required",
                                                    cluster.getNumberOfNodes(),
                                                    required,
                                                    required);

        createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = this.createClusterStores(cluster,
                                                                                                  "required",
                                                                                                  available,
                                                                                                  this.failureDetector,
                                                                                                  available
                                                                                                          - failures,
                                                                                                  1000L,
                                                                                                  failures,
                                                                                                  new VoldemortException("oops"));
        AsynchronousStore<ByteArray, byte[], byte[]> store = this.createAsyncStore(cluster,
                                                                                   storeDef,
                                                                                   stores);

        ByteArray key = getKey();
        byte[] value = getValue();

        testFailingFuture(store.submitGet(key, null), available, required, 0, failures);
        testFailingFuture(store.submitPut(key, new Versioned<byte[]>(value), null),
                          available,
                          required,
                          0,
                          failures);
        testFailingFuture(store.submitDelete(key, VersionFactory.newVersion()),
                          available,
                          required,
                          0,
                          failures);
        testFailingFuture(store.submitGetVersions(key), available, required, 0, failures);
    }

    @Test
    public void testCheckRequired() {
        ByteArray key = getKey();
        byte[] value = getValue();
        StoreDefinition storeDef = this.getStoreDef("required", 1, 1, 1);
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = this.createClusterStores(cluster,
                                                                                                  storeName,
                                                                                                  1,
                                                                                                  failureDetector,
                                                                                                  0,
                                                                                                  0,
                                                                                                  0,
                                                                                                  null);
        DistributedStore<Node, ByteArray, byte[], byte[]> store = this.buildDistributedStore(stores,
                                                                                             cluster,
                                                                                             storeDef);
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
        StoreDefinition storeDef = this.getStoreDef("invalid", 1, 1, 1);
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = this.createClusterStores(cluster,
                                                                                                  storeName,
                                                                                                  1,
                                                                                                  failureDetector,
                                                                                                  0,
                                                                                                  0,
                                                                                                  0,
                                                                                                  null);
        DistributedStore<Node, ByteArray, byte[], byte[]> store = buildDistributedStore(stores,
                                                                                        cluster,
                                                                                        storeDef);
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
            store.submitGet(null, null, empty, 1, 1);
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
            store.submitGetAll(null, null, 1, 1);
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
            store.submitPut(null, new Versioned<byte[]>(value), null, empty, 1, 1);
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
        int sleepy = 3;
        int good = cluster.getNumberOfNodes() - sleepy;
        StoreDefinition storeDef = this.getStoreDef("required",
                                                    cluster.getNumberOfNodes(),
                                                    good,
                                                    good);
        this.createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = this.createClusterStores(cluster,
                                                                                                  "required",
                                                                                                  cluster.getNumberOfNodes(),
                                                                                                  failureDetector,
                                                                                                  sleepy,
                                                                                                  2000,
                                                                                                  0,
                                                                                                  null);
        AsynchronousStore<ByteArray, byte[], byte[]> store = DistributedStoreFactory.asAsync(storeDef,
                                                                                             buildDistributedStore(stores,
                                                                                                                   cluster,
                                                                                                                   storeDef));

        ByteArray key = getKey();
        byte[] value = getValue();
        Versioned<byte[]> versioned = new Versioned<byte[]>(value);

        testReturnOnRequired(store.submitGet(key, null), 0, good);
        testReturnOnRequired(store.submitPut(key, versioned, null), 0, good);
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
        int required = 4;
        int failures = 2;
        int sleepy = 2;
        int good = cluster.getNumberOfNodes() - failures;

        StoreDefinition storeDef = this.getStoreDef("required",
                                                    cluster.getNumberOfNodes(),
                                                    cluster.getNumberOfNodes() - 2,
                                                    cluster.getNumberOfNodes() - 2);
        this.createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = this.createClusterStores(cluster,
                                                                                                  "invalid",
                                                                                                  cluster.getNumberOfNodes(),
                                                                                                  failureDetector,
                                                                                                  sleepy,
                                                                                                  1000,
                                                                                                  failures,
                                                                                                  new InvalidMetadataException("oops"));
        AsynchronousStore<ByteArray, byte[], byte[]> store = DistributedStoreFactory.asAsync(storeDef,
                                                                                             buildDistributedStore(stores,
                                                                                                                   cluster,
                                                                                                                   storeDef));

        ByteArray key = getKey();
        byte[] value = getValue();

        testFailingFuture(store.submitGet(key, null),
                          cluster.getNumberOfNodes(),
                          required,
                          1,
                          failures,
                          InvalidMetadataException.class);
        testFailingFuture(store.submitPut(key, new Versioned<byte[]>(value), null),
                          cluster.getNumberOfNodes(),
                          required,
                          1,
                          failures,
                          InvalidMetadataException.class);
        testFailingFuture(store.submitDelete(key, VersionFactory.newVersion()),
                          cluster.getNumberOfNodes(),
                          required,
                          1,
                          failures,
                          InvalidMetadataException.class);
        testFailingFuture(store.submitGetVersions(key),
                          cluster.getNumberOfNodes(),
                          required,
                          1,
                          failures,
                          InvalidMetadataException.class);
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
        failureDetector = createFailureDetector(BannagePeriodFailureDetector.class, this.cluster);
        return failureDetector;
    }
}
