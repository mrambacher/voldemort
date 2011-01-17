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

package voldemort.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalanceNodePlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

public class RebalanceUtilsTest extends TestCase {

    private static String storeDefFile = "test/common/voldemort/config/stores.xml";
    private static final ArrayList<Integer> empty = new ArrayList<Integer>();
    private Cluster currentCluster;
    private Cluster targetCluster;
    List<StoreDefinition> storeDefList;

    @Override
    public void setUp() {
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });

        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    public void testRebalancePlan() {
        System.out.println("testRebalancePlan() running...");
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true,
                                                                      null);
        // The store has a replication factor = 2
        // current cluster:
        // Node0 in zone 0 partitionList:[0, 1, 2, 3, 4, 5, 6, 7, 8],
        // Node1 in zone 0 partitionList:[],
        // 
        // target cluster :
        // Node0 in zone 0 partitionList:[0, 1, 4, 5, 6, 7, 8],
        // Node1 in zone 0 partitionList:[2, 3],
        //
        //
        // We have to move primaries but also replicas without deleting 2 and 3
        // otherwise you will lease the cluster unbalance based on replication
        // factor.
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node", 1, rebalancePlan.getRebalancingTaskQueue().size());
        for (RebalanceNodePlan rebalanceNodeInfo : rebalancePlan.getRebalancingTaskQueue()) {
            assertEquals("rebalanceInfo should have exactly one item", 1, rebalanceNodeInfo.getRebalanceTaskList().size());
            RebalancePartitionsInfo expected = new RebalancePartitionsInfo(rebalanceNodeInfo.getStealerNode(),
                                                                           0,
                                                                           Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8),
                                                                           empty,
                                                                           Arrays.asList(2, 3),
                                                                           RebalanceUtils.getStoreNames(storeDefList),
                                                                           new HashMap<String, String>(),
                                                                           new HashMap<String, String>(),
                                                                           0);

            assertEquals("rebalanceStealInfo should match", expected.toJsonString(), rebalanceNodeInfo.getRebalanceTaskList().get(0).toJsonString());
        }
    }

    public void testRebalanceDeletingFirstNode() {
        System.out.println("testRebalanceDeletingFirstNode() running...");
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 4 },
                { 1, 5 },
                { 2, 6 },
                { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                {},
                { 0, 1, 5 },
                { 4, 2, 6 },
                { 3, 7 } });

        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDef, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:112) - Current Cluster:
        // 0 - [0, 4] + [2, 3, 6, 7]
        // 1 - [1, 5] + [0, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 4, 5]
        // 3 - [3, 7] + [1, 2, 5, 6]
        //
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:113) - Target Cluster:
        // 0 - [] + []
        // 1 - [0, 1, 5] + [2, 3, 4, 6, 7]
        // 2 - [2, 4, 6] + [0, 1, 3, 5, 7]
        // 3 - [3, 7] + [0, 1, 2, 4, 5, 6]
        //
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:120) - Target deleted partitions: {0=[0, 2, 3, 4, 6, 7], 1=[], 2=[], 3=[]}
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:121) - Target stealing partitions: {0=[], 1=[2, 6], 2=[3, 7], 3=[0, 4]}
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 1, stealing primaries: [0]
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 1, stealing replicas: [2, 6]
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 2, stealing primaries: [4]
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 2, stealing replicas: [3, 7]
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 3, stealing primaries: []
        // [25.12.2010 13:32:50] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 3, stealing replicas: [0, 4]
        // Cluster Rebalancing Plan:
        //
        // StealerNode:1
        // RebalancePartitionsInfo: RebalancingStealInfo(1 <--- 0 partitions:[0] steal master partitions:[0] stores:[test])
        // getStealMasterPartitions(): [0]
        // getPartitionList(): [0]
        // getDeletePartitionsList(): [0, 2, 3, 6, 7]
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(1 <--- 2 partitions:[2, 6] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [2, 6]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:2
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 0 partitions:[4] steal master partitions:[4] stores:[test])
        // getStealMasterPartitions(): [4]
        // getPartitionList(): [4]
        // getDeletePartitionsList(): [4]
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 1 partitions:[3, 7] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [3, 7]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:3
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 1 partitions:[0, 4] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [0, 4]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //

        assertEquals("There should have exactly 3 rebalancing node", 3, rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 1 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                         0,
                                                                         Arrays.asList(0),
                                                                         Arrays.asList(0, 2, 3, 6, 7),
                                                                         Arrays.asList(0),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            RebalancePartitionsInfo b = new RebalancePartitionsInfo(1,
                                                                         2,
                                                                         Arrays.asList(2, 6),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 2 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                         0,
                                                                         Arrays.asList(4),
                                                                         Arrays.asList(4),
                                                                         Arrays.asList(4),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            RebalancePartitionsInfo b = new RebalancePartitionsInfo(2,
                                                                         1,
                                                                         Arrays.asList(3, 7),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                         1,
                                                                         Arrays.asList(0, 4),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

    }

    public void testRebalanceDeletingLastNode() {
        System.out.println("testRebalanceDeletingLastNode() running...");
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 4 },
                { 1, 5 },
                { 2, 6 },
                { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 3, 0, 4 },
                { 7, 1, 5 },
                { 2, 6 },
                {} });

        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDef, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:112) - Current Cluster:
        // 0 - [0, 4] + [2, 3, 6, 7]
        // 1 - [1, 5] + [0, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 4, 5]
        // 3 - [3, 7] + [1, 2, 5, 6]
        //
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:113) - Target Cluster:
        // 0 - [0, 3, 4] + [1, 2, 5, 6, 7]
        // 1 - [1, 5, 7] + [0, 2, 3, 4, 6]
        // 2 - [2, 6] + [0, 1, 3, 4, 5, 7]
        // 3 - [] + []
        //
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:120) - Target deleted partitions: {0=[], 1=[], 2=[], 3=[1, 2, 3, 5, 6, 7]}
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:121) - Target stealing partitions: {0=[1, 5], 1=[2, 6], 2=[3, 7], 3=[]}
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 0, stealing primaries: [3]
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 0, stealing replicas: [1, 5]
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 1, stealing primaries: [7]
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 1, stealing replicas: [2, 6]
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 2, stealing primaries: []
        // [25.12.2010 13:37:48] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 2, stealing replicas: [3, 7]
        // Cluster Rebalancing Plan:
        //
        // StealerNode:0
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 1 partitions:[1, 5] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [1, 5]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 3 partitions:[3] steal master partitions:[3] stores:[test])
        // getStealMasterPartitions(): [3]
        // getPartitionList(): [3]
        // getDeletePartitionsList(): [1, 2, 3, 5, 6]
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:1
        // RebalancePartitionsInfo: RebalancingStealInfo(1 <--- 0 partitions:[2, 6] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [2, 6]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(1 <--- 3 partitions:[7] steal master partitions:[7] stores:[test])
        // getStealMasterPartitions(): [7]
        // getPartitionList(): [7]
        // getDeletePartitionsList(): [7]
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:2
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 0 partitions:[3, 7] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [3, 7]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //

        assertEquals("There should have exactly 3 rebalancing node", 3, rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                         1,
                                                                         Arrays.asList(1, 5),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            RebalancePartitionsInfo b = new RebalancePartitionsInfo(0,
                                                                         3,
                                                                         Arrays.asList(3),
                                                                         Arrays.asList(1, 2, 3, 5, 6),
                                                                         Arrays.asList(3),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 1 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                         0,
                                                                         Arrays.asList(2, 6),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            RebalancePartitionsInfo b = new RebalancePartitionsInfo(1,
                                                                         3,
                                                                         Arrays.asList(7),
                                                                         Arrays.asList(7),
                                                                         Arrays.asList(7),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 2 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                         0,
                                                                         Arrays.asList(3, 7),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

    }

    public void testRebalanceDeletingMidleNode() {
        System.out.println("testRebalanceDeletingMidleNode() running...");
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 4 },
                { 1, 5 },
                { 2, 6 },
                { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 4 },
                { 2, 1, 5 },
                {},
                { 6, 3, 7 } });

        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDef, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        //
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:112) - Current Cluster:
        // 0 - [0, 4] + [2, 3, 6, 7]
        // 1 - [1, 5] + [0, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 4, 5]
        // 3 - [3, 7] + [1, 2, 5, 6]
        //
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:113) - Target Cluster:
        // 0 - [0, 4] + [1, 2, 3, 5, 6, 7]
        // 1 - [1, 2, 5] + [0, 3, 4, 6, 7]
        // 2 - [] + []
        // 3 - [3, 6, 7] + [0, 1, 2, 4, 5]
        //
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:120) - Target deleted partitions: {0=[], 1=[], 2=[0, 1, 2, 4, 5, 6], 3=[]}
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:121) - Target stealing partitions: {0=[1, 5], 1=[2, 6], 2=[], 3=[0, 4]}
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 0, stealing primaries: []
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 0, stealing replicas: [1, 5]
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 1, stealing primaries: [2]
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 1, stealing replicas: [6]
        // [25.12.2010 13:42:43] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 3, stealing primaries: [6]
        // [25.12.2010 13:42:44] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 3, stealing replicas: [0, 4]
        // Cluster Rebalancing Plan:
        //
        // StealerNode:0
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 1 partitions:[1, 5] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [1, 5]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:1
        // RebalancePartitionsInfo: RebalancingStealInfo(1 <--- 0 partitions:[6] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [6]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(1 <--- 2 partitions:[2] steal master partitions:[2] stores:[test])
        // getStealMasterPartitions(): [2]
        // getPartitionList(): [2]
        // getDeletePartitionsList(): [0, 1, 2, 4, 5]
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:3
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 0 partitions:[0, 4] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [0, 4]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 2 partitions:[6] steal master partitions:[6] stores:[test])
        // getStealMasterPartitions(): [6]
        // getPartitionList(): [6]
        // getDeletePartitionsList(): [6]
        // getUnbalancedStoreList(): [test]
        //
        assertEquals("There should have exactly 3 rebalancing node", 3, rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                         1,
                                                                         Arrays.asList(1, 5),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 1 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                         0,
                                                                         Arrays.asList(6),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            RebalancePartitionsInfo b = new RebalancePartitionsInfo(1,
                                                                         2,
                                                                         Arrays.asList(2),
                                                                         Arrays.asList(0, 1, 2, 4, 5),
                                                                         Arrays.asList(2),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 4),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            RebalancePartitionsInfo b = new RebalancePartitionsInfo(3,
                                                                         2,
                                                                         Arrays.asList(6),
                                                                         Arrays.asList(6),
                                                                         Arrays.asList(6),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }
    }

    public void testRebalancePlanWithReplicationChanges() {
        System.out.println("testRebalancePlanWithReplicationChanges() running...");
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6 }, { 7, 8, 9 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 3 }, { 4, 6 },
                { 7, 8, 9 }, { 1, 5 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDefList, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // - Current Cluster:
        // 0 - [0, 1, 2, 3] + [7, 8, 9]
        // 1 - [4, 5, 6] + [0, 1, 2, 3]
        // 2 - [7, 8, 9] + [4, 5, 6]
        //
        // - Target Cluster:
        // 0 - [0, 2, 3] + [1, 7, 8, 9]
        // 1 - [4, 6] + [2, 3, 5]
        // 2 - [7, 8, 9] + [6]
        // 3 - [1, 5] + [0, 4]
        //
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 0 partitions:[0, 1] steal master partitions:[1] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory, test-readonly-fetchfiles, test-readonly-versions, test-consistent, test-to-all, test-consistent-with-pref-list])
        // getStealMasterPartitions(): [1]
        // getPartitionList(): [0, 1]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory, test-readonly-fetchfiles, test-readonly-versions, test-consistent, test-to-all, test-consistent-with-pref-list]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 1 partitions:[4, 5] steal master partitions:[5] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory, test-readonly-fetchfiles, test-readonly-versions, test-consistent, test-to-all, test-consistent-with-pref-list])
        // getStealMasterPartitions(): [5]
        // getPartitionList(): [4, 5]
        // getDeletePartitionsList(): [0, 1]
        // getUnbalancedStoreList(): [users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory, test-readonly-fetchfiles, test-readonly-versions, test-consistent, test-to-all, test-consistent-with-pref-list]

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node", 1, rebalancePlan.getRebalancingTaskQueue().size());

        RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
        assertEquals("Rebalancing node 3 should have 2 entries", 2, nodePlan.getRebalanceTaskList().size());
        RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 1),
                                                                         empty,
                                                                         Arrays.asList(1),
                                                                         RebalanceUtils.getStoreNames(storeDefList),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
        RebalancePartitionsInfo b = new RebalancePartitionsInfo(3,
                                                                         1,
                                                                         Arrays.asList(4, 5),
                                                                         Arrays.asList(0, 1),
                                                                         Arrays.asList(5),
                                                                         RebalanceUtils.getStoreNames(storeDefList),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

        checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
    }

    /**
     * http://code.google.com/p/project-voldemort/issues/detail?id=288&q=
     * rebalance
     * (see comment-2)
     * 
     * 
     */
    public void testRebalanceAllReplicasBeingMigrated() {
        System.out.println("testRebalanceAllReplicasBeingMigrated() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] {
                { 0, 4 },
                { 2, 3 },
                { 1, 5 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 4 },
                { 2, 3 },
                { 1, 5 },
                { 0 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDef, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        // [25.12.2010 11:32:40] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:112) - Current Cluster:
        // 0 - [0, 4] + [1, 2, 3, 5]
        // 1 - [2, 3] + [0, 1, 4, 5]
        // 2 - [1, 5] + [0, 2, 3, 4]
        //
        // [25.12.2010 11:32:40] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:113) - Target Cluster:
        // 0 - [4] + [1, 2, 3]
        // 1 - [2, 3] + [0, 1, 5]
        // 2 - [1, 5] + [0, 2, 3, 4]
        // 3 - [0] + [4, 5]
        //
        // [25.12.2010 11:32:40] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:120) - Target deleted partitions: {0=[0, 5], 1=[4], 2=[], 3=null}
        // [25.12.2010 11:32:40] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:121) - Target stealing partitions: {0=[], 1=[], 2=[], 3=[0, 4, 5]}
        // [25.12.2010 11:32:40] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 3, stealing primaries: [0]
        // [25.12.2010 11:32:40] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 3, stealing replicas: [4, 5]
        // Cluster Rebalancing Plan:
        //
        // StealerNode:3
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 0 partitions:[0, 4] steal master partitions:[0] stores:[test])
        // getStealMasterPartitions(): [0]
        // getPartitionList(): [0, 4]
        // getDeletePartitionsList(): [0, 5]
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 1 partitions:[5] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [5]
        // getDeletePartitionsList(): [4]
        // getUnbalancedStoreList(): [test]

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly 1 rebalancing node", 1, rebalancePlan.getRebalancingTaskQueue().size());

        RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
        assertEquals("Rebalancing node 3 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
        RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 4),
                                                                         Arrays.asList(0, 5),
                                                                         Arrays.asList(0),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

        RebalancePartitionsInfo b = new RebalancePartitionsInfo(3,
                                                                         1,
                                                                         Arrays.asList(5),
                                                                         Arrays.asList(4),
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

        checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
    }

    public void testRebalanceDeletingNode() {
        System.out.println("testRebalanceDeletingNode() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 4 },
                { 1, 5 },
                { 2, 6 },
                { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 1, 4 },
                {},
                { 2, 5, 6 },
                { 3, 7 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDef, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:112) - Current Cluster:
        // 0 - [0, 4] + [2, 3, 6, 7]
        // 1 - [1, 5] + [0, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 4, 5]
        // 3 - [3, 7] + [1, 2, 5, 6]
        //
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:113) - Target Cluster:
        // 0 - [0, 1, 4] + [2, 3, 5, 6, 7]
        // 1 - [] + []
        // 2 - [2, 5, 6] + [0, 1, 3, 4, 7]
        // 3 - [3, 7] + [0, 1, 2, 4, 5, 6]
        //
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:120) - Target deleted partitions: {0=[], 1=[0, 1, 3, 4, 5, 7], 2=[], 3=[]}
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:121) - Target stealing partitions: {0=[1, 5], 1=[], 2=[3, 7], 3=[0, 4]}
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 0, stealing primaries: [1]
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 0, stealing replicas: [5]
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 2, stealing primaries: [5]
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 2, stealing replicas: [3, 7]
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 3, stealing primaries: []
        // [25.12.2010 11:33:04] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 3, stealing replicas: [0, 4]
        // Cluster Rebalancing Plan:
        //
        // StealerNode:0
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 1 partitions:[1] steal master partitions:[1] stores:[test])
        // getStealMasterPartitions(): [1]
        // getPartitionList(): [1]
        // getDeletePartitionsList(): [0, 1, 3, 4, 7]
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 2 partitions:[5] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [5]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:2
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 0 partitions:[3, 7] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [3, 7]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 1 partitions:[5] steal master partitions:[5] stores:[test])
        // getStealMasterPartitions(): [5]
        // getPartitionList(): [5]
        // getDeletePartitionsList(): [5]
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:3
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 0 partitions:[0, 4] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [0, 4]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]

        assertEquals("There should have exactly 3 rebalancing node", 3, rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                         1,
                                                                         Arrays.asList(1),
                                                                         Arrays.asList(0, 1, 3, 4, 7),
                                                                         Arrays.asList(1),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            RebalancePartitionsInfo b = new RebalancePartitionsInfo(0,
                                                                         2,
                                                                         Arrays.asList(5),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 2 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                         0,
                                                                         Arrays.asList(3, 7),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            RebalancePartitionsInfo b = new RebalancePartitionsInfo(2,
                                                                         1,
                                                                         Arrays.asList(5),
                                                                         Arrays.asList(5),
                                                                         Arrays.asList(5),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 4),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
    }

    public void testRebalanceDeletingOnePartition() {
        System.out.println("testRebalanceDeletingOnePartition() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 4, 8 },
                { 1, 5, 9 },
                { 2, 6, 10 },
                { 3, 7, 11 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 1, 4, 8 },
                { 9 },
                { 2, 5, 6, 10 },
                { 3, 7, 11 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster, targetCluster, storeDef, true, null);
        System.out.println("Plan partition distribution: " + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:112) - Current Cluster:
        // 0 - [0, 4, 8] + [2, 3, 6, 7, 10, 11]
        // 1 - [1, 5, 9] + [0, 3, 4, 7, 8, 11]
        // 2 - [2, 6, 10] + [0, 1, 4, 5, 8, 9]
        // 3 - [3, 7, 11] + [1, 2, 5, 6, 9, 10]
        //
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:113) - Target Cluster:
        // 0 - [0, 1, 4, 8] + [2, 3, 5, 6, 7, 10, 11]
        // 1 - [9] + [7, 8]
        // 2 - [2, 5, 6, 10] + [0, 1, 3, 4, 8, 9, 11]
        // 3 - [3, 7, 11] + [0, 1, 2, 4, 5, 6, 9, 10]
        //
        //
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:120) - Target deleted partitions: {0=[], 1=[0, 1, 3, 4, 5, 11], 2=[], 3=[]}
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.<init>(RebalanceClusterPlan.java:121) - Target stealing partitions: {0=[1, 5], 1=[], 2=[3, 11], 3=[0, 4]}
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 0, stealing primaries: [1]
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 0, stealing replicas: [5]
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 2, stealing primaries: [5]
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 2, stealing replicas: [3, 11]
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:220) - stealer id: 3, stealing primaries: []
        // [25.12.2010 11:33:33] DEBUG - (main) voldemort.client.rebalance.RebalanceClusterPlan.createRebalancePartitionsInfo(RebalanceClusterPlan.java:221) - stealer id: 3, stealing replicas: [0, 4]
        // Cluster Rebalancing Plan:
        //
        // StealerNode:0
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 1 partitions:[1] steal master partitions:[1] stores:[test])
        // getStealMasterPartitions(): [1]
        // getPartitionList(): [1]
        // getDeletePartitionsList(): [0, 1, 3, 4, 11]
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(0 <--- 2 partitions:[5] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [5]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:2
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 0 partitions:[3, 11] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [3, 11]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]
        //
        // RebalancePartitionsInfo: RebalancingStealInfo(2 <--- 1 partitions:[5] steal master partitions:[5] stores:[test])
        // getStealMasterPartitions(): [5]
        // getPartitionList(): [5]
        // getDeletePartitionsList(): [5]
        // getUnbalancedStoreList(): [test]
        //
        // StealerNode:3
        // RebalancePartitionsInfo: RebalancingStealInfo(3 <--- 0 partitions:[0, 4] steal master partitions:[] stores:[test])
        // getStealMasterPartitions(): []
        // getPartitionList(): [0, 4]
        // getDeletePartitionsList(): []
        // getUnbalancedStoreList(): [test]

        assertEquals("There should have exactly 3 rebalancing node", 3, rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                         1,
                                                                         Arrays.asList(1),
                                                                         Arrays.asList(0, 1, 3, 4, 11),
                                                                         Arrays.asList(1),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            RebalancePartitionsInfo b = new RebalancePartitionsInfo(0,
                                                                         2,
                                                                         Arrays.asList(5),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 2 should have 2 entry", 2, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                         0,
                                                                         Arrays.asList(3, 11),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

            RebalancePartitionsInfo b = new RebalancePartitionsInfo(2,
                                                                         1,
                                                                         Arrays.asList(5),
                                                                         Arrays.asList(5),
                                                                         Arrays.asList(5),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 4),
                                                                         empty,
                                                                         empty,
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
    }

    private void checkAllRebalanceInfoPresent(RebalanceNodePlan nodePlan,
                                              List<RebalancePartitionsInfo> rebalanceInfoList) {
        for (RebalancePartitionsInfo rebalanceInfo : rebalanceInfoList) {
            boolean match = false;
            for (RebalancePartitionsInfo nodeRebalanceInfo : nodePlan.getRebalanceTaskList()) {
                if (rebalanceInfo.getDonorId() == nodeRebalanceInfo.getDonorId()) {
                    assertEquals("partitions should match",
                                 true,
                                 compareList(rebalanceInfo.getPartitionList(), nodeRebalanceInfo.getPartitionList()));

                    assertEquals("delete partitions should match",
                                 true,
                                 compareList(rebalanceInfo.getDeletePartitionsList(), nodeRebalanceInfo.getDeletePartitionsList()));

                    assertEquals("store list should match",
                                 true,
                                 compareList(rebalanceInfo.getUnbalancedStoreList(), nodeRebalanceInfo.getUnbalancedStoreList()));

                    assertEquals("steal master partitions should match",
                                 true,
                                 compareList(rebalanceInfo.getStealMasterPartitions(), nodeRebalanceInfo.getStealMasterPartitions()));
                    match = true;
                }
            }

            assertNotSame("rebalancePartition Info " + rebalanceInfo
                                  + " should be present in the nodePlan "
                                  + nodePlan.getRebalanceTaskList(),
                          false,
                          match);
        }
    }

    public void testUpdateCluster() {
        System.out.println("testUpdateCluster() running...");
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    public void testRebalanceStealInfo() {
        System.out.println("testRebalanceStealInfo() running...");
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   new ArrayList<Integer>(0),
                                                                   Arrays.asList("test1", "test2"),
                                                                   new HashMap<String, String>(),
                                                                   new HashMap<String, String>(),
                                                                   0);
        // System.out.println("info:" + info.toString());

        assertEquals("RebalanceStealInfo fromString --> toString should match.",
                     info.toString(),
                     (RebalancePartitionsInfo.create(info.toJsonString())).toString());
    }

    private <T> boolean compareList(List<T> listA, List<T> listB) {
        // Both are null.
        if (listA == null && listB == null)
            return true;

        // At least one of them is null.
        if (listA == null || listB == null)
            return false;

        // If the size is different.
        if (listA.size() != listB.size())
            return false;

        // After checking the size we can call containsAll.
        // for example:
        //
        // listA =[0, 4, 5]
        // listB =[0, 4]
        //
        // This will return TRUE but they are not equal list.
        // 
        return listA.containsAll(listB);
    }

}
