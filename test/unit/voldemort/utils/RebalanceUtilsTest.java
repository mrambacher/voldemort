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
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true,
                                                                      null);

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
                                                                           Arrays.asList(2, 3),
                                                                           Arrays.asList(2, 3),
                                                                           Arrays.asList(2, 3),
                                                                           RebalanceUtils.getStoreNames(storeDefList),
                                                                           new HashMap<String, String>(),
                                                                           new HashMap<String, String>(),
                                                                           0);

            assertEquals("rebalanceStealInfo should match",
                         expected.toJsonString(),
                         rebalanceNodeInfo.getRebalanceTaskList().get(0).toJsonString());
        }
    }

    public void testRebalancePlanWithReplicationChanges() {
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6 }, { 7, 8, 9 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 3 }, { 4, 6 },
                { 7, 8, 9 }, { 1, 5 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true,
                                                                      null);

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node",
                     1,
                     rebalancePlan.getRebalancingTaskQueue().size());

        RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();

        assertEquals("Rebalancing node 3 should have 2 entries", 2, nodePlan.getRebalanceTaskList().size());
        ArrayList<Integer> empty = new ArrayList<Integer>();

        // http://code.google.com/p/project-voldemort/issues/detail?id=288&q=rebalance
        // (see comment 12)
        // In this test-case partition 1 and 5 should not be deleted.
        // If they are deleted then the replication factor 2 (in this case)
        // is violated.
        RebalancePartitionsInfo stealInfo0 = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 1),
                                                                         empty,
                                                                         Arrays.asList(1),
                                                                         RebalanceUtils.getStoreNames(storeDefList),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
        RebalancePartitionsInfo stealInfo1 = new RebalancePartitionsInfo(3,
                                                                         1,
                                                                         Arrays.asList(4, 5),
                                                                         empty,
                                                                         Arrays.asList(5),
                                                                         RebalanceUtils.getStoreNames(storeDefList),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

        checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(stealInfo0, stealInfo1));
    }

    /**
     * http://code.google.com/p/project-voldemort/issues/detail?id=288&q=
     * rebalance
     * (see comment-2)
     * 
     * 
     */
    public void testRebalanceAllReplicasBeingMigrated() {
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

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDef,
                                                                      true,
                                                                      null);

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node",
                     1,
                     rebalancePlan.getRebalancingTaskQueue().size());

        // System.out.println(rebalancePlan);
        RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();

        assertEquals("Rebalancing node 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
        RebalancePartitionsInfo stealInfo0 = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 4, 5),
                                                                         Arrays.asList(0),
                                                                         Arrays.asList(0),
                                                                         RebalanceUtils.getStoreNames(storeDef),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
        checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(stealInfo0));
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
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    public void testRebalanceStealInfo() {
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   new ArrayList<Integer>(0),
                                                                   Arrays.asList("test1", "test2"),
                                                                   new HashMap<String, String>(),
                                                                   new HashMap<String, String>(),
                                                                   0);
        System.out.println("info:" + info.toString());

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
