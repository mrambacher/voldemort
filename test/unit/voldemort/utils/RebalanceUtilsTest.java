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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Ignore;

import voldemort.ServerTestUtils;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalanceNodePlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

public class RebalanceUtilsTest extends TestCase {

    private static String storeDefFile = "test/common/voldemort/config/stores.xml";

    private Cluster currentCluster;
    private Cluster targetCluster;
    List<StoreDefinition> storeDefList;

    @Override
    public void setUp() {
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {}, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });

        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    public void testRebalancePlan() {
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      false);
        int[][] stealList = { {}, { 2, 3 } };

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node",
                     1,
                     rebalancePlan.getRebalancingTaskQueue().size());
        for (RebalanceNodePlan rebalanceNodeInfo : rebalancePlan.getRebalancingTaskQueue()) {
            assertEquals("rebalanceInfo should have exactly one item",
                         1,
                         rebalanceNodeInfo.getRebalanceTaskList().size());
            RebalancePartitionsInfo expected = new RebalancePartitionsInfo(rebalanceNodeInfo.getStealerNode(),
                                                                           0,
                                                                           listFromArray(stealList[rebalanceNodeInfo.getStealerNode()]),
                                                                           new ArrayList<Integer>(0),
                                                                           RebalanceUtils.getStoreNames(storeDefList),
                                                                           0);

            assertEquals("rebalanceStealInfo should match",
                         expected.toJsonString(),
                         rebalanceNodeInfo.getRebalanceTaskList().get(0).toJsonString());
        }
    }

    @Ignore
    public void testRebalancePlanWithReplicationChanges() {

        /**
         * This unit test is wrong. I was posted the issue in Voldemort at
         * http://code.google.com/p/project-voldemort/issues/detail?id=288
         * 
         * the stores in the unit test are 2/1/1
         * 
         * cluster.xml (before rebalance)
         * Node Primary Replica
         * 0 [0, 1, 2, 3] (7, 8, 9)
         * 1 [4, 5, 6] (0, 1, 2, 3)
         * 2 [7, 8, 9] (4, 5, 6)
         * 
         * ==============================
         * targetCluster.xml (after rebalance)
         * Node Primary Replica
         * 0 [0, 2, 3] (1, 7, 8, 9)
         * 1 [4, 6] (2, 3, 5)
         * 2 [7, 8, 9] (6)
         * 3 [1, 5] (0, 4)
         * 
         * Based on the mentioned unit-test, the remapping of the cluster is due
         * to migrating 2 primary partitions (1, 5) to a new node (Node3) this
         * will bring a set of other changes related to move the replicas too
         * (as shown above)
         * 
         * This is rebalancing plan as printed out by the program with my code
         * changes (I have posted 2: RebalanceClusterPlan.java.diff and
         * RebalanceClusterTools.diff)
         * 
         * StealerNode:0
         * RebalancingStealInfo(0 <--- 1 partitions:[1] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
         * 
         * StealerNode:1
         * RebalancingStealInfo(1 <--- 2 partitions:[5] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
         * 
         * StealerNode:3
         * RebalancingStealInfo(3 <--- 0 partitions:[1] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
         * RebalancingStealInfo(3 <--- 1 partitions:[0, 5] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
         * RebalancingStealInfo(3 <--- 2 partitions:[4] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
         */
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6 }, { 7, 8, 9 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 3 }, { 4, 6 },
                { 7, 8, 9 }, { 1, 5 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      false);
        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node", 3, rebalancePlan.getRebalancingTaskQueue().size());

        RebalanceNodePlan planStealer0 = rebalancePlan.getRebalancingTaskQueue().poll();
        RebalanceNodePlan planStealer1 = rebalancePlan.getRebalancingTaskQueue().poll();
        RebalanceNodePlan planStealer3 = rebalancePlan.getRebalancingTaskQueue().poll();

        assertEquals("Rebalancing node 0 should have 1 entries", 1, planStealer0.getRebalanceTaskList().size());
        assertEquals("Rebalancing node 1 should have 1 entries", 1, planStealer1.getRebalanceTaskList().size());
        assertEquals("Rebalancing node 3 should have 3 entries", 3, planStealer3.getRebalanceTaskList().size());

        // StealerNode:0
        // RebalancingStealInfo(0 <--- 1 partitions:[1] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
        RebalancePartitionsInfo stealer0Donor1 = new RebalancePartitionsInfo(0, 1, Arrays.asList(1), new ArrayList<Integer>(0), RebalanceUtils.getStoreNames(storeDefList), 0);

        // StealerNode:1
        // RebalancingStealInfo(1 <--- 2 partitions:[5] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
        RebalancePartitionsInfo stealer1Donor2 = new RebalancePartitionsInfo(1, 2, Arrays.asList(5), new ArrayList<Integer>(0), RebalanceUtils.getStoreNames(storeDefList), 0);

        // StealerNode:3
        // RebalancingStealInfo(3 <--- 0 partitions:[1] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
        // RebalancingStealInfo(3 <--- 1 partitions:[0, 5] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])
        // RebalancingStealInfo(3 <--- 2 partitions:[4] stores:[users, veggies, test-replication-memory, test-recovery-data, test-replication-persistent, test-readrepair-memory])

        RebalancePartitionsInfo stealer3Donor0 = new RebalancePartitionsInfo(3, 0, Arrays.asList(1), new ArrayList<Integer>(0), RebalanceUtils.getStoreNames(storeDefList), 0);
        RebalancePartitionsInfo stealer3Donor1 = new RebalancePartitionsInfo(3, 1, Arrays.asList(0, 5), new ArrayList<Integer>(0), RebalanceUtils.getStoreNames(storeDefList), 0);
        RebalancePartitionsInfo stealer3Donor2 = new RebalancePartitionsInfo(3, 2, Arrays.asList(4), new ArrayList<Integer>(0), RebalanceUtils.getStoreNames(storeDefList), 0);

        checkAllRebalanceInfoPresent(planStealer0, Arrays.asList(stealer0Donor1));
        checkAllRebalanceInfoPresent(planStealer1, Arrays.asList(stealer1Donor2));
        checkAllRebalanceInfoPresent(planStealer3, Arrays.asList(stealer3Donor0, stealer3Donor1, stealer3Donor2));

        // // the rebalancing plan should have exactly 1 node
        // assertEquals("There should have exactly one rebalancing node",
        // 1,
        // rebalancePlan.getRebalancingTaskQueue().size());
        //
        // RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
        //
        // assertEquals("Rebalancing node 3 should have 2 entries", 2, nodePlan.getRebalanceTaskList()
        // .size());
        // RebalancePartitionsInfo stealInfo0 = new RebalancePartitionsInfo(3,
        // 0,
        // Arrays.asList(0, 1),
        // new ArrayList<Integer>(0),
        // RebalanceUtils.getStoreNames(storeDefList),
        // 0);
        // RebalancePartitionsInfo stealInfo1 = new RebalancePartitionsInfo(3,
        // 1,
        // Arrays.asList(4, 5),
        // new ArrayList<Integer>(0),
        // RebalanceUtils.getStoreNames(storeDefList),
        // 0);
        //
        // checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(stealInfo0, stealInfo1));
    }

    private void checkAllRebalanceInfoPresent(RebalanceNodePlan nodePlan,
                                              List<RebalancePartitionsInfo> rebalanceInfoList) {
        for (RebalancePartitionsInfo rebalanceInfo : rebalanceInfoList) {
            boolean match = false;
            for (RebalancePartitionsInfo nodeRebalanceInfo : nodePlan.getRebalanceTaskList()) {
                if (rebalanceInfo.getDonorId() == nodeRebalanceInfo.getDonorId()) {
                    assertEquals("partitions should match",
                                 true,
                                 rebalanceInfo.getPartitionList()
                                              .containsAll(nodeRebalanceInfo.getPartitionList()));
                    assertEquals("delete partitions should match",
                                 true,
                                 rebalanceInfo.getDeletePartitionsList()
                                              .containsAll(nodeRebalanceInfo.getDeletePartitionsList()));
                    assertEquals("store list should match",
                                 true,
                                 rebalanceInfo.getUnbalancedStoreList()
                                              .containsAll(nodeRebalanceInfo.getUnbalancedStoreList()));
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
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    private List<Integer> listFromArray(int[] array) {
        List<Integer> list = new ArrayList<Integer>();
        for (int x : array) {
            list.add(x);
        }

        return list;
    }

    public void testRebalanceStealInfo() {
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   new ArrayList<Integer>(0),
                                                                   Arrays.asList("test1", "test2"),
                                                                   0);
        System.out.println("info:" + info.toString());

        assertEquals("RebalanceStealInfo fromString --> toString should match.",
                     info.toString(),
                     (RebalancePartitionsInfo.create(info.toJsonString())).toString());
    }

}
