/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalanceController {

    private static final int MAX_TRIES = 2;
    private static final String NL = System.getProperty("line.separator");
    private static final Logger logger = Logger.getLogger(RebalanceController.class);

    private final AdminClient adminClient;
    private final RebalanceClientConfig rebalanceConfig;

    public RebalanceController(String bootstrapUrl, RebalanceClientConfig rebalanceConfig) {
        this.adminClient = new AdminClient(bootstrapUrl, rebalanceConfig);
        this.rebalanceConfig = rebalanceConfig;
    }

    public RebalanceController(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new AdminClient(cluster, config);
        this.rebalanceConfig = config;
    }

    /**
     * Grabs the latest cluster definition and calls {@link #rebalance(voldemort.cluster.Cluster, voldemort.cluster.Cluster)}
     * 
     * @param targetCluster
     *            : target Cluster configuration
     */
    public void rebalance(final Cluster targetCluster) {
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                                     adminClient);
        rebalance(currentVersionedCluster.getValue(), targetCluster);
    }

    /**
     * Voldemort dynamic cluster membership rebalancing mechanism. <br>
     * Migrate partitions across nodes to manage changes in cluster membership. <br>
     * Takes target cluster as parameter, fetches the current cluster
     * configuration from the cluster, compares and makes a list of partitions
     * that eed to be transferred.<br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}
     * 
     * @param currentCluster
     *            : current cluster configuration
     * @param targetCluster
     *            : target cluster configuration
     */
    public void rebalance(Cluster currentCluster, final Cluster targetCluster) {
        if (logger.isDebugEnabled()) {
            logger.debug("Current Cluster configuration:" + currentCluster);
            logger.debug("Target Cluster configuration:" + targetCluster);
        }

        adminClient.setAdminClientCluster(currentCluster);

        // Cluster oldCluster = currentCluster;
        // Retrieve list of stores
        List<StoreDefinition> storesList = RebalanceUtils.getStoreNameList(currentCluster,
                                                                           adminClient);

        final RebalanceClusterPlan deleteOnlyPlan = new RebalanceClusterPlan(currentCluster,
                targetCluster,
                storesList,
                rebalanceConfig.isDeleteAfterRebalancingEnabled(),
                null);

        if (logger.isInfoEnabled()) {
            logger.info("Delete only plan:" + deleteOnlyPlan.toString());
            logger.info("Delete only plan distribution:" + deleteOnlyPlan.printPartitionDistribution());
        }

        // Add all new nodes to currentCluster
        currentCluster = getClusterWithNewNodes(currentCluster, targetCluster);
        adminClient.setAdminClientCluster(currentCluster);

        // Maintain nodeId to map of read-only store name to current version
        // dirs
        final Map<Integer, Map<String, String>> currentROStoreVersionsDirs = Maps.newHashMapWithExpectedSize(storesList.size());

        // Retrieve list of read-only stores
        List<String> readOnlyStores = Lists.newArrayList();
        for (StoreDefinition store : storesList) {
            if (store.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
                readOnlyStores.add(store.getName());
            }
        }

        // Retrieve current versions dirs for all nodes (old + new), required
        // for swapping at end
        if (readOnlyStores.size() > 0) {
            for (Node node : currentCluster.getNodes()) {
                currentROStoreVersionsDirs.put(node.getId(),
                                               adminClient.getROCurrentVersionDir(node.getId(),
                                                                                  readOnlyStores));
            }
        }

        final List<OrderedClusterTransition> clusterTransitionList = createClusterTransitions(currentCluster,
                                                                                       targetCluster,
                                                                                       storesList,
                                                                                       currentROStoreVersionsDirs);
        // Only shows the plan without doing rebalancing.
        if (rebalanceConfig.isShowPlanEnabled()) {
            for (OrderedClusterTransition clusterTransition : clusterTransitionList) {
                System.out.print(clusterTransition);
            }
            return;
        }

        if (clusterTransitionList.isEmpty()) {
            // Nothing to rebalance
            return;
        }

        for (final OrderedClusterTransition clusterTransition : clusterTransitionList) {
            final Cluster transitionTarget = clusterTransition.getTargetCluster();
            final RebalanceClusterPlan rebalanceClusterPlan = clusterTransition.getRebalanceClusterPlan();
            final List<RebalanceNodePlan> rebalanceNodePlanList = clusterTransition.getRebalanceNodePlanList();

            // Prints the original rebalace cluster plan.
            if (logger.isInfoEnabled()) {
                logger.info(clusterTransition);
            }

            if (rebalanceClusterPlan.getRebalancingTaskQueue().isEmpty()) {
                continue;
            }

            // Rebalance.
            rebalance(currentROStoreVersionsDirs, readOnlyStores, transitionTarget, rebalanceNodePlanList);
        }

        // Clean up.
        cleanUp(deleteOnlyPlan);

    }

    /**
     * Creates plans based on step-by-step transitions that cluster.xml goes through
     * to finally remapp itself to the targetCluster.xml *
     */
    private List<OrderedClusterTransition> createClusterTransitions(Cluster currentCluster, final Cluster targetCluster, final List<StoreDefinition> storesList, final Map<Integer, Map<String, String>> currentROStoreVersionsDirs) {
        final List<OrderedClusterTransition> clusterTransitionList = new LinkedList<OrderedClusterTransition>();
        final Map<Node, Set<Integer>> stealerToAddPrimaryPartitions = new HashMap<Node, Set<Integer>>();

        for (Node stealerNode : targetCluster.getNodes()) {
            stealerToAddPrimaryPartitions.put(stealerNode, getStealAddedPrimaries(currentCluster, targetCluster, stealerNode));
        }

        for (Node stealerNode : targetCluster.getNodes()) {
            // Checks if this stealer is stealing partition. If not then nothing to do
            Set<Integer> addedPartitions = stealerToAddPrimaryPartitions.get(stealerNode);
            if (addedPartitions == null || addedPartitions.isEmpty())
                continue;

            // If the node has existing partitions than the one added, then remove
            // the added partition, these are going to be added one by one.
            List<Integer> partitions = new ArrayList<Integer>(stealerNode.getPartitionIds());
            partitions.removeAll(addedPartitions);
            Node stealerNodeUpdated = createNode(stealerNode, partitions);

            // Now let's provision the added partitions one by one.
            // Creates a transition cluster for each added partition.
            for (Integer donatedPartition : addedPartitions) {
                Cluster targetTransition = createTransitionTargetCluster(currentCluster, stealerNodeUpdated, donatedPartition);
                stealerNodeUpdated = targetTransition.getNodeById(stealerNodeUpdated.getId());

                final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                        targetTransition,
                        storesList,
                        rebalanceConfig.isDeleteAfterRebalancingEnabled(),
                        currentROStoreVersionsDirs);

                clusterTransitionList.add(new OrderedClusterTransition(currentCluster, targetTransition, rebalanceClusterPlan));
                currentCluster = targetTransition;

            }
        }
        return clusterTransitionList;
    }

    private void rebalance(final Map<Integer, Map<String, String>> currentROStoreVersionsDirs, List<String> readOnlyStores, final Cluster transitionTarget, List<RebalanceNodePlan> rebalanceNodePlanList) {
        final List<Exception> failures = new ArrayList<Exception>();

        // All stealer and donor nodes
        final Set<Integer> nodeIds = Collections.synchronizedSet(new HashSet<Integer>());

        for (RebalanceNodePlan rebalanceNodePlan : rebalanceNodePlanList) {
            if (null == rebalanceNodePlan)
                continue;

            final int stealerNodeId = rebalanceNodePlan.getStealerNode();
            nodeIds.add(stealerNodeId);

            // In order to guarantee that a partition contains ALL keys sent while rebalancing is
            // in progress you have to guaranteed that any RebalancePartitionsInfo that migrated a "primary"
            // partition happens first than another RebalancePartitionsInfo that just copy "replicas".
            // This is due to the fact that propagation of new cluster is triggered only when a "primary"
            // is moved. If a "replica" is moved first then you will effectively copied the information
            // that was at that moment in the partition but not new incoming PUTs. Only a routing-strategy
            // will make ALL the PUTs goes to the right partitions (primary and replicas) after (an not
            // before) a new cluster.xml is propagated to all nodes.
            // Make this executor a single threaded to guaranteed sequentiality.
            for (final RebalancePartitionsInfo stealInfo : rebalanceNodePlan.getRebalanceTaskList()) {
                final int donorNodeId = stealInfo.getDonorId();
                nodeIds.add(donorNodeId);

                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Starting rebalancing for stealerNode: " + stealerNodeId + " with rebalanceInfo: " + stealInfo);
                    }

                    int rebalanceAsyncId = startNodeRebalancing(stealInfo);

                    try {
                        if (logger.isInfoEnabled()) {
                            logger.info("Commiting cluster changes, Async ID: " + rebalanceAsyncId +
                                    ", rebalancing for stealerNode: " + stealerNodeId +
                                    " with rebalanceInfo: " + stealInfo);
                        }

                        commitClusterChanges(adminClient.getAdminClientCluster().getNodeById(stealerNodeId), stealInfo);

                    } catch (Exception e) {
                        if (-1 != rebalanceAsyncId) {
                            adminClient.stopAsyncRequest(stealInfo.getStealerId(), rebalanceAsyncId);
                        }
                        logger.error("Commiting the cluster has failed. Async ID:" + rebalanceAsyncId + " - " + e.getMessage(), e);
                        throw e;
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Waitting ForCompletion: startNodeRebalancing (rebalanceAsyncId:" + rebalanceAsyncId + ") - for RebalancePartitionIndo: " + stealInfo);
                    }

                    adminClient.waitForCompletion(stealInfo.getStealerId(),
                                                  rebalanceAsyncId,
                                                  rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                                  TimeUnit.SECONDS);
                    if (logger.isInfoEnabled()) {
                        logger.info("Succesfully finished rebalance for: " + stealInfo);
                    }

                } catch (UnreachableStoreException e) {
                    logger.error("StealerNode " + stealerNodeId + " is unreachable, please make sure it is up and running. - " + e.getMessage(), e);
                    failures.add(e);
                } catch (VoldemortRebalancingException e) {
                    logger.error(e);
                    for (Exception cause : e.getCauses()) {
                        logger.error(cause);
                    }
                    failures.add(e);
                } catch (Exception e) {
                    logger.error("Rebalancing task failed with exception - " + e.getMessage(), e);
                    failures.add(e);
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Finished rebalanceNodePlanList");
        }

        // If everything successful, swap the read-only stores
        if (failures.size() == 0 && readOnlyStores.size() > 0) {
            logger.info("Swapping stores " + readOnlyStores + " on " + nodeIds);
            ExecutorService swapExecutors = RebalanceUtils.createExecutors(transitionTarget.getNumberOfNodes());
            for (final Integer nodeId : nodeIds) {
                swapExecutors.submit(new Runnable() {

                    public void run() {
                        Map<String, String> storeDirs = currentROStoreVersionsDirs.get(nodeId);

                        try {
                            logger.info("Swapping read-only stores on node " + nodeId);
                            adminClient.swapStoresAndCleanState(nodeId, storeDirs);
                            logger.info("Successfully swapped on node " + nodeId);
                        } catch (Exception e) {
                            logger.error("Failed swapping on node " + nodeId, e);
                        }

                    }
                });
            }

            try {
                RebalanceUtils.executorShutDown(swapExecutors, rebalanceConfig.getRebalancingClientTimeoutSeconds());
            } catch (Exception e) {
                logger.error("Interrupted swapping executor ", e);
                return;
            }
        }
    }

    private int startNodeRebalancing(RebalancePartitionsInfo rebalanceSubTask) {
        int nTries = 0;
        AlreadyRebalancingException exception = null;

        while (nTries < MAX_TRIES) {
            nTries++;
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("startNodeRebalancing - for: " + rebalanceSubTask);
                }
                int rebalanceNode = adminClient.rebalanceNode(rebalanceSubTask);
                if (logger.isDebugEnabled()) {
                    logger.debug("startNodeRebalancing - for: " + rebalanceSubTask + ", rebalanceNode: " + rebalanceNode);
                }

                return rebalanceNode;
            } catch (AlreadyRebalancingException e) {
                logger.info("Node " + rebalanceSubTask.getStealerId()
                            + " is currently rebalancing will wait till it finish.");
                adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                              MetadataStore.SERVER_STATE_KEY,
                                              VoldemortState.NORMAL_SERVER.toString(),
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                exception = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing at node "
                                     + rebalanceSubTask.getStealerId() + " with rebalanceInfo:"
                                     + rebalanceSubTask, exception);
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }

    /* package level function to ease of unit testing */

    /**
     * Does an atomic commit or revert for the intended partitions ownership
     * changes and modifies adminClient with the updatedCluster.<br>
     * Creates new cluster metadata by moving partitions list passed in as
     * parameter rebalanceStealInfo and propagates it to all nodes.<br>
     * Revert all changes if failed to copy on required nodes (stealer and
     * donor).<br>
     * Holds a lock untill the commit/revert finishes.
     * 
     * @param stealerNode
     *            Node copy data from
     * @param rebalanceStealInfo
     *            Current rebalance sub task
     * @throws Exception
     *             If we are unable to propagate the cluster definition to
     *             donor and stealer
     */
    void commitClusterChanges(Node stealerNode, RebalancePartitionsInfo rebalanceStealInfo)
            throws Exception {
        synchronized (adminClient) {
            Cluster currentCluster = adminClient.getAdminClientCluster();
            Node donorNode = currentCluster.getNodeById(rebalanceStealInfo.getDonorId());

            Versioned<Cluster> latestCluster = RebalanceUtils.getLatestCluster(Arrays.asList(donorNode.getId(),
                                                                                             rebalanceStealInfo.getStealerId()),
                                                                               adminClient);
            Version latest = latestCluster.getVersion();

            // apply changes and create new updated cluster.
            // use steal master partitions to update cluster increment clock
            // version on stealerNodeId
            Cluster updatedCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                         stealerNode,
                                                                         donorNode,
                                                                         rebalanceStealInfo.getStealMasterPartitions());
            latest.incrementClock(stealerNode.getId(), System.currentTimeMillis());
            try {
                // propagates changes to all nodes.
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latest,
                                                Arrays.asList(stealerNode.getId(),
                                                              rebalanceStealInfo.getDonorId()));

                // set new cluster in adminClient
                adminClient.setAdminClientCluster(updatedCluster);
            } catch (Exception e) {
                // revert cluster changes.
                updatedCluster = currentCluster;
                latest.incrementClock(stealerNode.getId(), System.currentTimeMillis());
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latest,
                                                new ArrayList<Integer>());
                logger.error("Exception during comming changes in the cluster.xml - " + e.getMessage(), e);
                throw e;
            }

            adminClient.setAdminClientCluster(updatedCluster);
        }
    }

    private Cluster getClusterWithNewNodes(Cluster currentCluster, Cluster targetCluster) {
        ArrayList<Node> newNodes = new ArrayList<Node>();
        for (Node node : targetCluster.getNodes()) {
            if (!RebalanceUtils.containsNode(currentCluster, node.getId())) {
                // add stealerNode with empty partitions list
                newNodes.add(RebalanceUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        return RebalanceUtils.updateCluster(currentCluster, newNodes);
    }

    /**
     * For a particular stealer node find all the partitions it will steal
     * 
     * @param currentCluster
     *            The cluster definition of the existing cluster
     * @param targetCluster
     *            The target cluster definition
     * @param stealerNode
     *            The stealer node
     * @return Returns a list of partitions which this stealer node will get
     */
    private Set<Integer> getStealAddedPrimaries(final Cluster currentCluster, final Cluster targetCluster, final Node stealerNode) {
        int stealNodeId = stealerNode.getId();
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId).getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if (RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return new TreeSet<Integer>(targetList);
    }

    /**
     * Creates a new cluster by adding a donated partition to a new or existing
     * node.
     * 
     * @param currentCluster current cluster used to copy from.
     * @param stealerNode now or existing node being updated.
     * @param donatedPartition partition donated to the <code>stealerNode</code>
     * @return
     */
    private Cluster createTransitionTargetCluster(final Cluster currentCluster, Node stealerNode, final Integer donatedPartition) {
        // Gets the donor Node that owns this donated partition
        Node donorNode = getNodeByPartition(currentCluster, donatedPartition);

        // Removes the node from the list,
        final List<Node> nodes = new ArrayList<Node>(currentCluster.getNodes());
        nodes.remove(donorNode);
        nodes.remove(stealerNode);

        // Update the list of partitions for this node
        donorNode = removePartitionToNode(donorNode, donatedPartition);
        stealerNode = addPartitionToNode(stealerNode, donatedPartition);

        // Add the updated nodes (donor and stealer).
        nodes.add(donorNode);
        nodes.add(stealerNode);

        // After the stealer & donor were fixed recreate the cluster.
        // Sort the nodes so they will appear in the same order all the time.
        Collections.sort(nodes);
        return new Cluster(currentCluster.getName(), nodes);
    }

    private Node addPartitionToNode(Node node, Integer donatedPartition) {
        List<Integer> deepCopy = new ArrayList<Integer>(node.getPartitionIds());
        deepCopy.add(donatedPartition);
        return createNode(node, deepCopy);
    }

    private Node removePartitionToNode(final Node node, final Integer donatedPartition) {
        List<Integer> deepCopy = new ArrayList<Integer>(node.getPartitionIds());
        deepCopy.remove(donatedPartition);
        return createNode(node, deepCopy);
    }

    /**
     * Returns the Node associated to the provided partition.
     * 
     * @param currentCluster
     * @param donatedPartition
     * @return Node that owns <code>donatedPartition</code>
     */
    private Node getNodeByPartition(Cluster currentCluster, Integer donatedPartition) {
        Map<Integer, Node> partitionToNode = new HashMap<Integer, Node>();
        for (Node node : currentCluster.getNodes()) {
            for (Integer partition : node.getPartitionIds()) {
                partitionToNode.put(partition, node);
            }
        }
        return partitionToNode.get(donatedPartition);
    }

    private Node createNode(int id, String host, int httpPort, int socketPort, int adminPort, int zoneId, List<Integer> partitions) {
        return new Node(id,
                    host,
                    httpPort,
                    socketPort,
                    adminPort,
                    zoneId,
                    partitions);
    }

    private Node createNode(final Node node, final List<Integer> partitions) {
        return createNode(node.getId(),
                            node.getHost(),
                            node.getHttpPort(),
                            node.getSocketPort(),
                            node.getAdminPort(),
                            node.getZoneId(),
                            partitions);
    }

    private void cleanUp(final RebalanceClusterPlan deleteOnlyPlan) {
        Queue<RebalanceNodePlan> queue = deleteOnlyPlan.getRebalancingTaskQueue();
        RebalanceNodePlan[] plans = new RebalanceNodePlan[queue.size()];
        queue.toArray(plans);
        for (RebalanceNodePlan plan : plans) {
            for (RebalancePartitionsInfo pinfo : plan.getRebalanceTaskList()) {
                List<Integer> deletePartitionsList = pinfo.getDeletePartitionsList();
                int donorId = pinfo.getDonorId();

                // Checks if you have delete partitions, if not then is a no-op.
                if (deletePartitionsList == null || deletePartitionsList.isEmpty())
                    continue;

                for (String storeName : pinfo.getUnbalancedStoreList()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Deleting partitions: " + deletePartitionsList + " from NodeId: " + donorId + " - store named: " + storeName);
                    }
                    try {
                        getAdminClient().deletePartitions(donorId, storeName, deletePartitionsList, null);
                    } catch (Exception ignored) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Partition already deleted " + deletePartitionsList +
                                    ", from NodeId: " + donorId + ", store named: " + storeName +
                                    " - " + ignored.getMessage(), ignored);
                        }
                    }
                }
            }
        }
    }

    /**
     * Immutable representation of a cluster transition that guarantees the order of the
     * rebalance task and subtask ({@link RebalanceNodePlan}, {@link RebalancePartitionsInfo})
     * respectively.
     * 
     * The order of the task/subtaks is needed to make sure that migration of primary
     * partitions are before any other
     * ( {@link RebalanceNodePlan} , {@link RebalancePartitionsInfo}) instances.
     * 
     * Propagation of a cluster is triggered by detecting that a stealer node is
     * given a "primary" partition only. In this case if we execute a task/subtask
     * that moves only "replicas" will only have the side effect of "copying"
     * the data saved at this point in time in the replica but not new operations
     * that enter the system afterward. On the other hand, by guaranteeing that
     * all task/subtask that migrate primary partitions happen first then afterward
     * any task/subtask that copy replicas will have all the data inclusive
     * new incoming transactions entered on the system.
     * 
     * 
     * @author jocohen
     * @since Jan 10, 2011
     */
    private static class OrderedClusterTransition {
        private final Cluster origenCluster;
        private final Cluster targetCluster;
        private final RebalanceClusterPlan rebalanceClusterPlan;
        private final List<RebalanceNodePlan> rebalanceNodePlanList;
        private String printedContent;

        public OrderedClusterTransition(final Cluster currentCluster, final Cluster targetCluster, final RebalanceClusterPlan rebalanceClusterPlan) {
            this.origenCluster = currentCluster;
            this.targetCluster = targetCluster;
            this.rebalanceClusterPlan = rebalanceClusterPlan;
            this.rebalanceNodePlanList = orderedClusterPlan(rebalanceClusterPlan);
        }

        public Cluster getCurrentCluster() {
            return origenCluster;
        }

        public Cluster getTargetCluster() {
            return targetCluster;
        }

        public List<RebalanceNodePlan> getRebalanceNodePlanList() {
            return rebalanceNodePlanList;
        }

        public RebalanceClusterPlan getRebalanceClusterPlan() {
            return rebalanceClusterPlan;
        }

        /**
         * Lazy initialization. Thanks to the immutable characteristic
         * of this class, only one creations of this printable content
         * of this class is sufficient.
         */
        @Override
        public String toString() {
            if (printedContent == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("*** Cluster transition:").append(NL).append(getCurrentCluster()).append(NL);
                sb.append("*** Target transition:").append(NL).append(getTargetCluster()).append(NL);
                sb.append("*** Rebalance Cluster Plan:").append(NL).append(getRebalanceClusterPlan()).append(NL);
                sb.append("*** Partition distribution:").append(NL).append(getRebalanceClusterPlan().printPartitionDistribution()).append(NL);
                sb.append("*** Ordered rebalance node plan:").append(NL).append(printRebalanceNodePlan(getRebalanceNodePlanList())).append(NL);
                printedContent = sb.toString();
            }
            return printedContent;
        }

        private List<RebalanceNodePlan> orderedClusterPlan(final RebalanceClusterPlan rebalanceClusterPlan) {
            Queue<RebalanceNodePlan> rebalancingTaskQueue = rebalanceClusterPlan.getRebalancingTaskQueue();
            RebalanceNodePlan[] array = new RebalanceNodePlan[rebalancingTaskQueue.size()];
            rebalancingTaskQueue.toArray(array);

            List<RebalanceNodePlan> plans = new ArrayList<RebalanceNodePlan>();
            for (RebalanceNodePlan rebalanceNodePlan : array) {
                List<RebalancePartitionsInfo> orderedRebalancePartitionsInfos = orderedPartitionInfos(rebalanceNodePlan);
                plans.add(new RebalanceNodePlan(rebalanceNodePlan.getStealerNode(), orderedRebalancePartitionsInfos));
            }

            return orderedNodePlans(plans);
        }

        /**
         * Ordering {@link RebalancePartitionsInfo} for a single stealer in such
         * a way that guarantees that primary (master) partitions will be before
         * any other instance of {@link RebalancePartitionsInfo} that only
         * moves/migrates replica partitions only.
         * 
         * @param rebalanceNodePlan the plan for a single stealer.
         * @return list of ordered {@link RebalancePartitionsInfo}.
         */
        private List<RebalancePartitionsInfo> orderedPartitionInfos(final RebalanceNodePlan rebalanceNodePlan) {
            List<RebalancePartitionsInfo> listPrimaries = new ArrayList<RebalancePartitionsInfo>();
            List<RebalancePartitionsInfo> listReplicas = new ArrayList<RebalancePartitionsInfo>();

            List<RebalancePartitionsInfo> pinfos = rebalanceNodePlan.getRebalanceTaskList();

            for (RebalancePartitionsInfo pinfo : pinfos) {
                List<Integer> stealMasterPartitions = pinfo.getStealMasterPartitions();
                if (stealMasterPartitions != null && !stealMasterPartitions.isEmpty()) {
                    listPrimaries.add(pinfo);
                }
                else {
                    listReplicas.add(pinfo);
                }
            }

            listPrimaries.addAll(listReplicas);
            return listPrimaries;
        }

        private List<RebalanceNodePlan> orderedNodePlans(List<RebalanceNodePlan> plans) {
            List<RebalanceNodePlan> first = new ArrayList<RebalanceNodePlan>();
            List<RebalanceNodePlan> second = new ArrayList<RebalanceNodePlan>();

            for (RebalanceNodePlan plan : plans) {
                boolean found = false;
                for (RebalancePartitionsInfo pinfo : plan.getRebalanceTaskList()) {
                    List<Integer> stealMasterPartitions = pinfo.getStealMasterPartitions();
                    if (stealMasterPartitions != null && !stealMasterPartitions.isEmpty()) {
                        found = true;
                        break;
                    }
                }

                if (found) {
                    first.add(plan);
                }
                else {
                    second.add(plan);
                }
            }
            first.addAll(second);
            return first;
        }

        private String printRebalanceNodePlan(List<RebalanceNodePlan> rebalanceNodePlanList) {
            StringBuilder sb = new StringBuilder();
            for (RebalanceNodePlan plan : rebalanceNodePlanList) {
                for (RebalancePartitionsInfo pinfo : plan.getRebalanceTaskList()) {
                    sb.append(pinfo).append(NL);
                }
            }
            return sb.toString();
        }

    }
}
