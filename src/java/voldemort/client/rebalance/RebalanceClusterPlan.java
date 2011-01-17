package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.Multimap;

/**
 * Compares the currentCluster configuration with the desired
 * targetConfiguration and returns a map of Target node-id to map of source
 * node-ids and partitions desired to be stolen/fetched.
 * 
 */
public class RebalanceClusterPlan {
    private final static Logger logger = Logger.getLogger(RebalanceClusterPlan.class);

    /** New Line - OS independent. */
    private final static String NL = System.getProperty("line.separator");

    /**
     * Queue with the rebalance plan to be executed as a result or remapping the
     * cluster
     */
    private Queue<RebalanceNodePlan> optimizedRebalanceTaskQueue;

    /**
     * Complete rebalance plan. Contains all the "movements" of primaries and
     * replicas
     * partitions as a consequence of remapping the cluster.
     */
    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    /** The list of store definitions to rebalance */
    private final List<StoreDefinition> storeDefList;

    /** Current cluster */
    private final Cluster currentCluster;

    /**
     * Compares the currentCluster configuration with the desired
     * targetConfiguration and builds a map of Target node-id to map of source
     * node-ids and partitions desired to be stolen/fetched.
     * 
     * @param currentCluster
     *            The current cluster definition
     * @param targetCluster
     *            The target cluster definition
     * @param storeDefList
     *            The list of store definitions to rebalance
     * @param deleteDonorPartition
     *            Delete the RW partition on the donor side after
     *            rebalance
     * @param currentROStoreVersionsDirs
     *            A mapping of nodeId to map of store names to version ids
     */
    public RebalanceClusterPlan(Cluster currentCluster,
                                Cluster targetCluster,
                                List<StoreDefinition> storeDefList,
                                boolean deleteDonorPartition,
                                Map<Integer, Map<String, String>> currentROStoreVersionsDirs) {
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        this.storeDefList = storeDefList;
        this.currentCluster = currentCluster;

        if (currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        for (Node node : targetCluster.getNodes()) {
            List<RebalancePartitionsInfo> rebalanceNodeList = getRebalanceNodeTask(currentCluster,
                                                                                   targetCluster,
                                                                                   RebalanceUtils.getStoreNames(storeDefList),
                                                                                   node.getId(),
                                                                                   deleteDonorPartition);
            if (rebalanceNodeList.size() > 0) {
                if (currentROStoreVersionsDirs != null && currentROStoreVersionsDirs.size() > 0) {
                    for (RebalancePartitionsInfo partitionsInfo : rebalanceNodeList) {
                        partitionsInfo.setStealerNodeROStoreToDir(currentROStoreVersionsDirs.get(partitionsInfo.getStealerId()));
                        partitionsInfo.setDonorNodeROStoreToDir(currentROStoreVersionsDirs.get(partitionsInfo.getDonorId()));
                    }
                }
                rebalanceTaskQueue.offer(new RebalanceNodePlan(node.getId(), rebalanceNodeList));
            }
        }
    }

    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        if (optimizedRebalanceTaskQueue == null) {
            optimizedRebalanceTaskQueue = optimizeRebalanceTaskQueue(rebalanceTaskQueue);
        }
        return optimizedRebalanceTaskQueue;
    }

    /**
     * Optimizes the rebalance plan.<br>
     * 
     * Optimizations can be made to original rebalance plan
     * <code>rebalanceTaskQueue</code>.
     * This method servers as a hub for individual method calls that
     * will contribute to the optimization of the plan.
     * 
     * @param queue rebalance plan to be optimized.
     * @return optimized rebalance plan
     */
    private Queue<RebalanceNodePlan> optimizeRebalanceTaskQueue(final Queue<RebalanceNodePlan> queue) {
        // toString(rebalanceTaskQueue);
        Queue<RebalanceNodePlan> optimizedQueue = avoidDeleteNeededPartitions(queue);
        if (logger.isDebugEnabled()) {
            logger.debug("Avoid deleting needed partitions: " + toString(optimizedQueue));
        }

        optimizedQueue = avoidCopyExistingPartitions(optimizedQueue);
        if (logger.isDebugEnabled()) {
            logger.debug("Avoid copying existing partitions: " + toString(optimizedQueue));
        }

        optimizedQueue = avoidContactingMultipleNodes(optimizedQueue);
        if (logger.isDebugEnabled()) {
            logger.debug("Avoid contacting multiple nodes: " + toString(optimizedQueue));
        }

        return optimizedQueue;
    }

    /**
     * Primary partition 'Z' is donated from node 'A' to node 'B' and the same
     * partition 'Z' could comes back now as a Replica to node 'A'. Under this
     * scenario this partition will be deleted if Rebalance CLI
     * {@link RebalanceCLI} is launched without the option 'no-delete' (which is
     * the default)
     * 
     * The side effect of this is that 'Z' will be deleted all together if the
     * Rebalance Plan executes the migration of the Primary partition after the
     * Rebalance Plan that copy the partition as Replica.
     * 
     * @param queue to be checked
     * @return queue modified
     */
    private Queue<RebalanceNodePlan> avoidDeleteNeededPartitions(final Queue<RebalanceNodePlan> queue) {
        int planSize = queue.size();
        RebalanceNodePlan[] oldNodePlans = new RebalanceNodePlan[planSize];
        RebalanceNodePlan[] newNodePlans = new RebalanceNodePlan[planSize];

        queue.toArray(oldNodePlans);
        queue.toArray(newNodePlans);

        // Take the original rebalance plan and loop through the Rebalance Node
        // Plans
        for (RebalanceNodePlan oldNodePlan : oldNodePlans) {
            for (RebalancePartitionsInfo oldPartitionInfo : oldNodePlan.getRebalanceTaskList()) {

                final List<Integer> oldPartitionsList = oldPartitionInfo.getPartitionList();
                if (oldPartitionsList == null || oldPartitionsList.size() == 0)
                    continue;

                final int oldStealerId = oldPartitionInfo.getStealerId();

                // Now look thrown the newNodePlan and remove the partition that
                // is check to be deleted only if it's a replica in other plans.
                for (RebalanceNodePlan newNodePlan : newNodePlans) {
                    for (RebalancePartitionsInfo newPartitionInfo : newNodePlan.getRebalanceTaskList()) {

                        // Check if you have a Donor that happens to be a
                        // Stealer in another NodePlan.
                        int newDonorId = newPartitionInfo.getDonorId();
                        if (newDonorId == oldStealerId) {
                            newPartitionInfo.getDeletePartitionsList().removeAll(oldPartitionsList);
                        }
                    }

                }

            }
        }
        List<RebalanceNodePlan> collection = Arrays.asList(newNodePlans);
        return new ConcurrentLinkedQueue<RebalanceNodePlan>(collection);
    }

    /**
     * Avoid copying partitions that exist in the node. Primary partitions could
     * be moved as a result of rebalance. Also a replica of the same partition
     * can be migrated back to the same node. In this case an optimization can
     * be made by avoiding this unnecessary migration.
     * 
     * @param queue to be checked
     * @return queue modified
     */
    private Queue<RebalanceNodePlan> avoidCopyExistingPartitions(final Queue<RebalanceNodePlan> queue) {
        RebalanceNodePlan[] rebalanceNodePlans = new RebalanceNodePlan[queue.size()];
        queue.toArray(rebalanceNodePlans);

        // Take the original rebalance plan and loop through the Rebalance Node
        // Plans
        for (RebalanceNodePlan rebalanceNodePlan : rebalanceNodePlans) {
            for (RebalancePartitionsInfo rebalancePartitionsInfo : rebalanceNodePlan.getRebalanceTaskList()) {

                final int stealerId = rebalancePartitionsInfo.getStealerId();
                final List<Integer> copyPartitionsList = new ArrayList<Integer>(rebalancePartitionsInfo.getPartitionList());

                // Try to see if moving this partition to this node is
                // unnecessary.
                // If this node has the partition already no need to copy it.
                for (Integer partition : copyPartitionsList) {
                    if (RebalanceUtils.containsNode(currentCluster, stealerId)) {
                        if (currentCluster.getNodeById(stealerId).getPartitionIds().contains(partition)) {
                            rebalancePartitionsInfo.getPartitionList().remove(partition);
                        }
                    }
                }
            }
        }

        // Delete the plans that now don't have any partition to move.
        List<RebalanceNodePlan> rebalanceNodePlansList = new LinkedList<RebalanceNodePlan>(Arrays.asList(rebalanceNodePlans));
        Iterator<RebalanceNodePlan> iterator = rebalanceNodePlansList.iterator();
        while (iterator.hasNext()) {
            if (removeIfEmpty(iterator.next().getRebalanceTaskList())) {
                iterator.remove();
            }
        }

        return new ConcurrentLinkedQueue<RebalanceNodePlan>(rebalanceNodePlansList);
    }

    /**
     * In case that you have 2 or more donors, the logic could check if only one
     * of these donor is enoght to steal from. If the donor has primary or
     * replica partitions that we are about to take from another donor we can
     * minimize network activity by only stealing from one node only all the
     * partitions that we need.
     * 
     * @param queue to be checked
     * @return queue modified
     */
    private Queue<RebalanceNodePlan> avoidContactingMultipleNodes(Queue<RebalanceNodePlan> queue) {
        RebalanceNodePlan[] rebalanceNodePlans = new RebalanceNodePlan[queue.size()];
        queue.toArray(rebalanceNodePlans);

        for (RebalanceNodePlan nodePlan : rebalanceNodePlans) {
            // Let's look how can we avoid talking to too many donors.
            Map<Integer, Set<Integer>> donorToDonatedPartitionMap = createDonorToDonatedPartitionMap(nodePlan);
            Set<Integer> donorIdsSet = new TreeSet<Integer>(donorToDonatedPartitionMap.keySet());
            int numDonors = donorIdsSet.size();
            Integer[] a = new Integer[numDonors];
            donorIdsSet.toArray(a);

            for (int i = 0; i < numDonors - 1; i++) {
                int donorIdA = a[i];
                List<Integer> partitionIds = null;
                if (RebalanceUtils.containsNode(currentCluster, donorIdA)) {
                    Node donorNode = currentCluster.getNodeById(donorIdA);
                    partitionIds = donorNode.getPartitionIds();
                }

                for (int j = i + 1; j < numDonors; j++) {
                    int donorIdB = a[j];
                    Iterator<Integer> iter = donorToDonatedPartitionMap.get(donorIdB).iterator();
                    while (iter.hasNext()) {
                        Integer donatedPartition = iter.next();
                        if (partitionIds.contains(donatedPartition)) {
                            donorToDonatedPartitionMap.get(donorIdA).add(donatedPartition);
                            iter.remove();
                        }
                    }

                }
            }

            // /

            Iterator<RebalancePartitionsInfo> iter = nodePlan.getRebalanceTaskList().iterator();

            while (iter.hasNext()) {
                RebalancePartitionsInfo partitionInfo = iter.next();
                int donorId = partitionInfo.getDonorId();
                Set<Integer> set = donorToDonatedPartitionMap.get(donorId);
                if (set == null || set.isEmpty()) {
                    iter.remove();
                }
                else {
                    partitionInfo.getPartitionList().clear();
                    partitionInfo.getPartitionList().addAll(set);
                }

            }

            //
        }
        // for (RebalanceNodePlan nodePlan : rebalanceNodePlans) {
        // }

        List<RebalanceNodePlan> collection = Arrays.asList(rebalanceNodePlans);
        return new ConcurrentLinkedQueue<RebalanceNodePlan>(collection);

    }

    private Map<Integer, Set<Integer>> createDonorToDonatedPartitionMap(final RebalanceNodePlan rebalanceNodePlan) {
        Map<Integer, Set<Integer>> donorToDonatedPartitionMap = new LinkedHashMap<Integer, Set<Integer>>();
        for (RebalancePartitionsInfo partitionInfo : rebalanceNodePlan.getRebalanceTaskList()) {
            int donorId = partitionInfo.getDonorId();
            Set<Integer> donatedPartitionsList = new TreeSet<Integer>();
            donatedPartitionsList.addAll(partitionInfo.getPartitionList());
            donatedPartitionsList.addAll(partitionInfo.getStealMasterPartitions());
            if (donorToDonatedPartitionMap.containsKey(donorId)) {
                donorToDonatedPartitionMap.get(donorId).addAll(donatedPartitionsList);
            }
            else {
                donorToDonatedPartitionMap.put(donorId, donatedPartitionsList);
            }
        }
        return donorToDonatedPartitionMap;
    }

    /**
     * Remove instances of {@link RebalancePartitionsInfo} if they empty
     * based on {{@link #isEmpty(RebalancePartitionsInfo)}
     * 
     * @param list
     *            entries to be checked.
     * @return TRUE is list is empty.
     */
    private boolean removeIfEmpty(List<RebalancePartitionsInfo> list) {
        if (list == null || list.isEmpty())
            return true;

        Iterator<RebalancePartitionsInfo> iterator = list.iterator();
        while (iterator.hasNext()) {
            if (isEmpty(iterator.next())) {
                iterator.remove();
            }
        }

        return list.isEmpty();
    }

    private boolean isEmpty(final RebalancePartitionsInfo rebalancePartitionsInfo) {
        return (rebalancePartitionsInfo.getStealMasterPartitions().isEmpty() &&
                rebalancePartitionsInfo.getPartitionList().isEmpty() && rebalancePartitionsInfo.getDeletePartitionsList().isEmpty());
    }

    /**
     * For a particular stealer node retrieves a list of plans corresponding to
     * each donor node.
     * 
     * @param currentCluster
     *            The cluster definition of the current cluster
     * @param targetCluster
     *            The cluster definition of the target cluster
     * @param storeList
     *            The list of stores
     * @param stealNodeId
     *            The node id of the stealer node
     * @param deleteDonorPartition
     *            Delete the donor partitions after rebalance
     * @return List of plans per donor node
     */
    private List<RebalancePartitionsInfo> getRebalanceNodeTask(Cluster currentCluster,
                                                               Cluster targetCluster,
                                                               List<String> storeList,
                                                               int stealNodeId,
                                                               boolean deleteDonorPartition) {
        Map<Integer, Integer> currentPartitionsToNodeMap = RebalanceUtils.getCurrentPartitionMapping(currentCluster);
        List<Integer> stealList = getStealList(currentCluster, targetCluster, stealNodeId);

        Map<Integer, List<Integer>> donorToDonatedMasterPartitions = getDonorToDonatedMasterPartitions(stealList,
                                                                                   currentPartitionsToNodeMap);

        // copies partitions needed to satisfy new replication mapping.
        // these partitions should be copied but not deleted from original node.
        Map<Integer, List<Integer>> donorToDonatedReplicaPartitions = getDonorToDonatedReplicaPartitions(currentCluster,
                                                                                     targetCluster,
                                                                                     stealNodeId,
                                                                                     currentPartitionsToNodeMap);

        List<RebalancePartitionsInfo> stealInfoList = new ArrayList<RebalancePartitionsInfo>();
        for (Node donorNode : currentCluster.getNodes()) {
            Set<Integer> stealPartitions = new HashSet<Integer>();
            Set<Integer> deletePartitions = new HashSet<Integer>();
            // create Set for steal master partitions
            Set<Integer> stealMasterPartitions = new HashSet<Integer>();

            if (donorToDonatedMasterPartitions.containsKey(donorNode.getId())) {
                stealPartitions.addAll(donorToDonatedMasterPartitions.get(donorNode.getId()));
                // add one steal master partition
                stealMasterPartitions.addAll(donorToDonatedMasterPartitions.get(donorNode.getId()));
                if (deleteDonorPartition)
                    deletePartitions.addAll(donorToDonatedMasterPartitions.get(donorNode.getId()));
            }

            if (donorToDonatedReplicaPartitions.containsKey(donorNode.getId())) {
                stealPartitions.addAll(donorToDonatedReplicaPartitions.get(donorNode.getId()));
            }

            if (stealPartitions.size() > 0) {
                stealInfoList.add(new RebalancePartitionsInfo(stealNodeId,
                                                              donorNode.getId(),
                                                              new ArrayList<Integer>(stealPartitions),
                                                              new ArrayList<Integer>(deletePartitions),
                                                              new ArrayList<Integer>(stealMasterPartitions),
                                                              storeList,
                                                              new HashMap<String, String>(),
                                                              new HashMap<String, String>(),
                                                              0));
            }
        }

        return stealInfoList;
    }

    /**
     * For a particular stealer node find all the partitions it will steal
     * 
     * @param currentCluster
     *            The cluster definition of the existing cluster
     * @param targetCluster
     *            The target cluster definition
     * @param stealNodeId
     *            The id of the stealer node
     * @return Returns a list of partitions which this stealer node will get
     */
    private List<Integer> getStealList(Cluster currentCluster,
                                       Cluster targetCluster,
                                       int stealNodeId) {
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if (RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return targetList;
    }

    /**
     * For a particular stealer node id returns a mapping of donor node ids to
     * their respective partition ids which we need to steal due to the change
     * of replication
     * 
     * @param currentCluster
     *            Current cluster definition
     * @param targetCluster
     *            Cluster definition of the target
     * @param stealNodeId
     *            The node id of the stealer node
     * @param currentPartitionsToNodeMap
     *            The mapping of current partitions to
     *            their nodes
     * @return Map of donor node ids to their partitions they'll donate
     */
    private Map<Integer, List<Integer>> getDonorToDonatedReplicaPartitions(Cluster currentCluster,
                                                              Cluster targetCluster,
                                                              int stealNodeId,
                                                              Map<Integer, Integer> currentPartitionsToNodeMap) {
        Map<Integer, List<Integer>> replicationMapping = new HashMap<Integer, List<Integer>>();
        List<Integer> targetList = targetCluster.getNodeById(stealNodeId).getPartitionIds();

        // get changing replication mapping
        RebalanceClusterTool clusterTool = new RebalanceClusterTool(currentCluster,
                                                                    RebalanceUtils.getMaxReplicationStore(this.storeDefList));
        Multimap<Integer, Pair<Integer, Integer>> replicationChanges = clusterTool.getRemappedReplicas(targetCluster);

        for (final Entry<Integer, Pair<Integer, Integer>> entry : replicationChanges.entries()) {
            int newReplica = entry.getValue().getSecond();
            if (targetList.contains(newReplica)) {
                // stealerNode need to replicate some new partition now.
                int donorNode = currentPartitionsToNodeMap.get(entry.getValue().getFirst());
                if (donorNode != stealNodeId)
                    createAndAdd(replicationMapping, donorNode, entry.getKey());
            }
        }

        return replicationMapping;
    }

    /**
     * Converts a list of partitions ids which a stealer is going to receive to
     * a map of donor node ids to the corresponding partitions
     * 
     * @param stealList
     *            The partitions ids going to be stolen
     * @param currentPartitionsToNodeMap
     *            Mapping of current partitions to their
     *            respective nodes ids
     * @return Returns a mapping of donor node ids to the partitions being
     *         stolen
     */
    private Map<Integer, List<Integer>> getDonorToDonatedMasterPartitions(List<Integer> stealList,
                                                                 Map<Integer, Integer> currentPartitionsToNodeMap) {
        HashMap<Integer, List<Integer>> stealPartitionsMap = new HashMap<Integer, List<Integer>>();
        for (int p : stealList) {
            int donorNode = currentPartitionsToNodeMap.get(p);
            createAndAdd(stealPartitionsMap, donorNode, p);
        }

        return stealPartitionsMap;
    }

    private void createAndAdd(Map<Integer, List<Integer>> map, int key, int value) {
        // create array if needed
        if (!map.containsKey(key)) {
            map.put(key, new ArrayList<Integer>());
        }

        // add partition to list.
        map.get(key).add(value);
    }

    @Override
    public String toString() {
        if (rebalanceTaskQueue.isEmpty()) {
            return "Cluster is already balanced, No rebalancing needed";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Cluster Rebalancing Plan:").append(NL);
        builder.append(toString(getRebalancingTaskQueue()));
        return builder.toString();
    }

    public String toString(Queue<RebalanceNodePlan> queue) {
        if (queue == null || queue.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder(NL);
        for (RebalanceNodePlan nodePlan : queue) {
            builder.append("StealerNode:" + nodePlan.getStealerNode()).append(NL);
            for (RebalancePartitionsInfo rebalancePartitionsInfo : nodePlan.getRebalanceTaskList()) {
                builder.append("\t RebalancePartitionsInfo: " + rebalancePartitionsInfo).append(NL);
                builder.append("\t\t getStealMasterPartitions(): " + rebalancePartitionsInfo.getStealMasterPartitions()).append(NL);
                builder.append("\t\t getPartitionList(): " + rebalancePartitionsInfo.getPartitionList()).append(NL);
                builder.append("\t\t getDeletePartitionsList(): " + rebalancePartitionsInfo.getDeletePartitionsList()).append(NL);
                builder.append("\t\t getUnbalancedStoreList(): " + rebalancePartitionsInfo.getUnbalancedStoreList()).append(NL).append(NL);
            }
        }

        return builder.toString();
    }

}
