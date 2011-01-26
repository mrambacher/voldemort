package voldemort.store.routed;

import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.MutableStoreVerifier;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SlopSerializer;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.store.ForceFailStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.ThreadedStore;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedParallelStore;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.failuredetector.FailureDetectingStore;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.HintedHandoffStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class HintedHandoffTest extends TestCase {

    private final static Logger logger = Logger.getLogger(HintedHandoffTest.class);
    private static final Serializer<Slop> slopSerializer = new SlopSerializer();

    private final static String STORE_NAME = "test";
    private final static String SLOP_STORE_NAME = "slop";

    private final static int NUM_THREADS = 5;
    private final static int NUM_NODES_TOTAL = 9;
    private final static int NUM_NODES_FAILED = 4;

    private final static int REPLICATION_FACTOR = 3;
    private final static int P_READS = 1;
    private final static int R_READS = 1;
    private final static int P_WRITES = 2;
    private final static int R_WRITES = 1;

    private final static int KEY_LENGTH = 32;
    private final static int VALUE_LENGTH = 32;

    private final Class<? extends FailureDetector> failureDetectorCls = BannagePeriodFailureDetector.class;
    private final HintedHandoffStrategyType hintRoutingStrategy;

    private final Map<Integer, ForceFailStore<ByteArray, byte[], byte[]>> failingStores = new ConcurrentHashMap<Integer, ForceFailStore<ByteArray, byte[], byte[]>>();
    private final Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> asyncStores = new ConcurrentHashMap<Node, AsynchronousStore<ByteArray, byte[], byte[]>>();
    private final Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> slopStores = new ConcurrentHashMap<Node, AsynchronousStore<ByteArray, byte[], byte[]>>();
    private final List<StreamingSlopPusherJob> slopPusherJobs = Lists.newLinkedList();
    private final Multimap<ByteArray, Integer> keysToNodes = HashMultimap.create();
    private final Map<ByteArray, ByteArray> keyValues = Maps.newHashMap();

    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ThreadPoolExecutor routedStoreThreadPool;
    private RoutingStrategy strategy;
    private HintedHandoffStore handoffStore;
    private Store<ByteArray, byte[], byte[]> store;

    public HintedHandoffTest(HintedHandoffStrategyType hintRoutingStrategy) {
        this.hintRoutingStrategy = hintRoutingStrategy;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { /*
                                               * {HintedHandoffStrategyType.
                                               * CONSISTENT_STRATEGY },
                                               */
        { HintedHandoffStrategyType.ANY_STRATEGY },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY } });
    }

    private StoreDefinition getStoreDef(String storeName,
                                        int replicationFactor,
                                        int preads,
                                        int rreads,
                                        int pwrites,
                                        int rwrites,
                                        String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setReplicationFactor(replicationFactor)
                                           .setPreferredReads(preads)
                                           .setRequiredReads(rreads)
                                           .setPreferredWrites(pwrites)
                                           .setRequiredWrites(rwrites)
                                           .setHintedHandoffStrategy(hintRoutingStrategy)
                                           .build();
    }

    protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                      StoreDefinition storeDef) {
        return DistributedParallelStore.create(stores, storeDef, true);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        cluster = getNineNodeCluster();
        storeDef = getStoreDef(STORE_NAME,
                               REPLICATION_FACTOR,
                               P_READS,
                               R_READS,
                               P_WRITES,
                               R_WRITES,
                               RoutingStrategyType.CONSISTENT_STRATEGY);
        routedStoreThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_THREADS);
        Map<Node, StoreRepository> repositories = Maps.newHashMap();
        MetadataStore metadata = ServerTestUtils.createMetadataStore(cluster, storeDef);
        for(Node node: cluster.getNodes()) {
            StoreRepository storeRepo = new StoreRepository();
            VoldemortException e = new UnreachableStoreException("Node down");

            InMemoryStore<ByteArray, byte[], byte[]> memStore = new InMemoryStore<ByteArray, byte[], byte[]>(STORE_NAME);
            LoggingStore<ByteArray, byte[], byte[]> loggingStore = new LoggingStore<ByteArray, byte[], byte[]>(memStore);
            ForceFailStore<ByteArray, byte[], byte[]> failingStore = new ForceFailStore<ByteArray, byte[], byte[]>(loggingStore,
                                                                                                                   e);
            failingStores.put(node.getId(), failingStore);
            AsynchronousStore<ByteArray, byte[], byte[]> async = ThreadedStore.create(failingStore,
                                                                                      routedStoreThreadPool);
            asyncStores.put(node, FailureDetectingStore.create(node, failureDetector, async));
            StorageEngine<ByteArray, byte[], byte[]> memory = ServerTestUtils.createMemoryEngine(metadata,
                                                                                                 TestUtils.getStoreDef(SLOP_STORE_NAME,
                                                                                                                       InMemoryStorageConfiguration.TYPE_NAME));

            SlopStorageEngine slopStore = new SlopStorageEngine(memory, cluster);
            slopStores.put(node,
                           FailureDetectingStore.create(node,
                                                        failureDetector,
                                                        ThreadedStore.create(slopStore,
                                                                             routedStoreThreadPool)));
            storeRepo.addLocalStore(AsyncUtils.asStore(async));
            storeRepo.setSlopStore(slopStore);
            repositories.put(node, storeRepo);
        }

        setFailureDetector(failingStores);

        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            StoreRepository storeRepo = repositories.get(node);

            for(Map.Entry<Node, AsynchronousStore<ByteArray, byte[], byte[]>> entry: asyncStores.entrySet()) {
                storeRepo.addNodeStore(entry.getKey().getId(), AsyncUtils.asStore(entry.getValue()));
            }
            MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster,
                                                                              Lists.newArrayList(storeDef));
            StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(storeRepo,
                                                                       metadataStore,
                                                                       failureDetector,
                                                                       ServerTestUtils.createServerConfigWithDefs(false,
                                                                                                                  nodeId,
                                                                                                                  TestUtils.createTempDir()
                                                                                                                           .getAbsolutePath(),
                                                                                                                  cluster,
                                                                                                                  Lists.newArrayList(storeDef),
                                                                                                                  new Properties()));
            slopPusherJobs.add(pusher);
        }

        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = this.buildDistributedStore(asyncStores,
                                                                                                   storeDef);

        distributor = new RoutableStore<byte[], byte[]>(distributor,
                                                        cluster,
                                                        storeDef,
                                                        failureDetector,
                                                        0);
        this.strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
        handoffStore = new HintedHandoffStore(distributor,
                                              storeDef,
                                              cluster,
                                              slopStores,
                                              failureDetector,
                                              0);
        this.store = AsyncUtils.asStore(handoffStore);

        generateData();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        if(routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();
    }

    @Test
    public void testHintedHandoff() throws Exception {
        Set<Integer> failedNodes = getFailedNodes();
        Multimap<Integer, ByteArray> failedKeys = populateStore(failedNodes);

        Map<ByteArray, byte[]> dataInSlops = Maps.newHashMap();
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys, Slop.Operation.PUT);
        for(Map.Entry<Node, AsynchronousStore<ByteArray, byte[], byte[]>> slops: slopStores.entrySet()) {
            AsynchronousStore<ByteArray, byte[], byte[]> slopStore = slops.getValue();
            Map<ByteArray, List<Versioned<byte[]>>> res = slopStore.submitGetAll(slopKeys, null)
                                                                   .get();
            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: res.entrySet()) {
                Slop slop = slopSerializer.toObject(entry.getValue().get(0).getValue());
                dataInSlops.put(slop.getKey(), slop.getValue());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        for(Map.Entry<Integer, ByteArray> failedKey: failedKeys.entries()) {
            byte[] expected = keyValues.get(failedKey.getValue()).get();
            byte[] actual = dataInSlops.get(failedKey.getValue());

            assertNotNull("data should be stored in the slop for key = " + failedKey.getValue(),
                          actual);
            assertEquals("correct size be stored in slop", actual.length, expected.length);
            assertEquals("correct should be stored in slop", 0, ByteUtils.compare(actual, expected));
        }

    }

    private Set<ByteArray> makeSlopKeys(Multimap<Integer, ByteArray> failedKeys,
                                        Slop.Operation operation) {
        Set<ByteArray> slopKeys = Sets.newHashSet();

        for(Map.Entry<Integer, ByteArray> entry: failedKeys.entries()) {
            byte[] opCode = new byte[] { operation.getOpCode() };
            byte[] spacer = new byte[] { (byte) 0 };
            byte[] storeName = ByteUtils.getBytes(STORE_NAME, "UTF-8");
            byte[] nodeIdBytes = new byte[ByteUtils.SIZE_OF_INT];
            ByteUtils.writeInt(nodeIdBytes, entry.getKey(), 0);
            ByteArray slopKey = new ByteArray(ByteUtils.cat(opCode,
                                                            spacer,
                                                            storeName,
                                                            spacer,
                                                            nodeIdBytes,
                                                            spacer,
                                                            entry.getValue().get()));
            slopKeys.add(slopKey);
        }
        return slopKeys;
    }

    @Test
    @Ignore
    public void testSlopPushers() throws Exception {
        Set<Integer> failedNodes = getFailedNodes();
        Multimap<Integer, ByteArray> failedKeys = populateStore(failedNodes);
        Thread.sleep(5000);
        ExecutorService executor = Executors.newFixedThreadPool(slopPusherJobs.size());
        final CountDownLatch latch = new CountDownLatch(slopPusherJobs.size());
        for(final StreamingSlopPusherJob job: slopPusherJobs) {
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        if(logger.isTraceEnabled())
                            logger.trace("Started slop pusher job " + job);
                        job.run();
                        if(logger.isTraceEnabled())
                            logger.trace("Finished slop pusher job " + job);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        Thread.sleep(5000);
        for(Map.Entry<Integer, ByteArray> entry: failedKeys.entries()) {
            List<Versioned<byte[]>> values = store.get(entry.getValue(), null);

            assertTrue("slop entry should be pushed for " + entry.getValue() + ", preflist "
                       + keysToNodes.get(entry.getValue()), values.size() > 0);
            assertEquals("slop entry should be correct for " + entry.getValue(),
                         keyValues.get(entry.getValue()),
                         new ByteArray(values.get(0).getValue()));
        }
    }

    @Test
    @Ignore
    public void testDeleteHandoff() throws Exception {
        populateStore(Sets.<Integer> newHashSet());

        Map<ByteArray, Version> versions = Maps.newHashMap();
        for(ByteArray key: keyValues.keySet())
            versions.put(key, store.get(key, null).get(0).getVersion());

        Set<Integer> failedNodes = getFailedNodes();
        Multimap<Integer, ByteArray> failedKeys = ArrayListMultimap.create();

        for(ByteArray key: keysToNodes.keySet()) {
            List<Node> nodes = strategy.routeRequest(key.get());
            for(Node node: nodes) {
                if(failedNodes.contains(node.getId())) {
                    failedKeys.put(node.getId(), key);
                    break;
                }
            }
        }

        for(Map.Entry<Integer, ByteArray> failedKey: failedKeys.entries()) {
            try {
                store.delete(failedKey.getValue(), versions.get(failedKey.getValue()));
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }

        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys, Slop.Operation.DELETE);
        Set<ByteArray> keysInSlops = Sets.newHashSet();

        Thread.sleep(5000);

        for(AsynchronousStore<ByteArray, byte[], byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<byte[]>>> res = slopStore.submitGetAll(slopKeys, null)
                                                                   .get();
            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: res.entrySet()) {
                Slop slop = slopSerializer.toObject(entry.getValue().get(0).getValue());
                keysInSlops.add(slop.getKey());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        for(Map.Entry<Integer, ByteArray> failedKey: failedKeys.entries())
            assertTrue("delete operation for " + failedKey.getValue() + " should be handed off",
                       keysInSlops.contains(failedKey.getValue()));

    }

    private Set<Integer> getFailedNodes() {
        Set<Integer> failedNodes = new CopyOnWriteArraySet<Integer>();
        Random rand = new Random();
        for(int i = 0; i < NUM_NODES_FAILED; i++) {
            int n = rand.nextInt(NUM_NODES_TOTAL);
            failedNodes.add(n);
        }

        for(int node: failedNodes)
            getForceFailStore(node).setFail(true);

        if(logger.isTraceEnabled())
            logger.trace("Failing requests to " + failedNodes);

        return failedNodes;
    }

    private void stopFailing(Collection<Integer> failedNodes) {
        for(int node: failedNodes)
            getForceFailStore(node).setFail(false);
    }

    private Multimap<Integer, ByteArray> populateStore(Set<Integer> failedNodes) {
        long started = System.currentTimeMillis();
        Multimap<Integer, ByteArray> failedKeys = ArrayListMultimap.create();
        List<DistributedFuture<Node, Version>> futures = new ArrayList<DistributedFuture<Node, Version>>(failedKeys.size());
        for(ByteArray key: keysToNodes.keySet()) {
            List<Node> nodes = strategy.routeRequest(key.get());
            for(Node node: nodes) {
                if(failedNodes.contains(node.getId())) {
                    failedKeys.put(node.getId(), key);
                    break;
                }
            }

            try {
                Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key).get());
                DistributedFuture<Node, Version> future = handoffStore.submitPut(key,
                                                                                 versioned,
                                                                                 null,
                                                                                 nodes,
                                                                                 storeDef.getPreferredWrites(),
                                                                                 storeDef.getRequiredWrites());
                futures.add(future);
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }
        for(DistributedFuture<Node, Version> future: futures) {
            try {
                future.complete();
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }
        System.out.println("Populated store in " + (System.currentTimeMillis() - started)
                           + " with " + failedKeys.size());
        return failedKeys;
    }

    private void generateData() {
        for(int i = 0; i < 2; i++) {
            Set<Integer> nodesCovered = Sets.newHashSet();
            while(nodesCovered.size() < NUM_NODES_TOTAL) {
                ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
                byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

                if(randomKey.length() > 0 && randomValue.length > 0) {
                    for(Node node: strategy.routeRequest(randomKey.get())) {
                        keysToNodes.put(randomKey, node.getId());
                        nodesCovered.add(node.getId());
                    }

                    keyValues.put(randomKey, new ByteArray(randomValue));
                }
            }
        }
    }

    private void setFailureDetector(Map<Integer, ? extends Store<ByteArray, byte[], byte[]>> subStores)
            throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig();
        failureDetectorConfig.setImplementationClassName(failureDetectorCls.getName());
        failureDetectorConfig.setBannagePeriod(500);
        failureDetectorConfig.setNodes(cluster.getNodes());
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }

    public ForceFailStore<ByteArray, byte[], byte[]> getForceFailStore(int nodeId) {
        return failingStores.get(nodeId);
    }
}