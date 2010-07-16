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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * @author jay
 * 
 */
public class RoutedStore extends ReadRepairStore<Integer> {

    private static final Logger logger = LogManager.getLogger(RoutedStore.class);

    private RoutingStrategy routingStrategy;
    private final FailureDetector failureDetector;

    /**
     * Create a RoutedStoreClient
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param cluster The cluster definition.
     * @param storeDef The store defintion.
     * @param numberOfThreads The number of threads in the threadpool
     * @param repairReads Should reads be repaired.
     * @param timeoutMs Timeout for operations.
     */
    public RoutedStore(String name,
                       Map<Integer, Store<ByteArray, byte[]>> innerStores,
                       Cluster cluster,
                       StoreDefinition storeDef,
                       int numberOfThreads,
                       boolean repairReads,
                       long timeout,
                       TimeUnit units,
                       FailureDetector failureDetector) {
        this(name,
             innerStores,
             cluster,
             storeDef,
             Executors.newFixedThreadPool(numberOfThreads),
             failureDetector,
             repairReads,
             timeout,
             units,
             SystemTime.INSTANCE);

        if(logger.isDebugEnabled()) {
            logger.debug("Routed Store created for store: " + name);
        }
    }

    /**
     * Create a RoutedStoreClient
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param cluster The cluster definition.
     * @param storeDef The store defintion.
     * @param repairReads Should reads be repaired.
     * @param threadPool The threadpool to use
     * @param timeoutMs Timeout for operations.
     * @param nodeBannageMs Period for which nodes are marked unavailable on
     *        failure.
     * @param time time used in vector clocks
     */
    public RoutedStore(String name,
                       Map<Integer, Store<ByteArray, byte[]>> stores,
                       Cluster cluster,
                       StoreDefinition storeDef,
                       ExecutorService threadPool,
                       FailureDetector failureDetector,
                       boolean repairReads,
                       long timeout,
                       TimeUnit units,
                       Time time) {
        super(name, stores, storeDef, threadPool, timeout, units, time, repairReads);

        if(logger.isDebugEnabled()) {
            logger.debug("Routed Store created for store: " + name
                         + " using the following strategy.");
        }
        this.failureDetector = failureDetector;
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
    }

    public FailureDetector getFailureDetector() {
        return this.failureDetector;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROUTING_STRATEGY:
                return this.routingStrategy;
            case VERSION_INCREMENTING:
                return true;
            case FAILURE_DETECTOR:
                return getFailureDetector();
            default:
                return super.getCapability(capability);
        }
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        final List<Integer> nodes = availableNodes(routingStrategy.routeRequest(key.get()));

        // quickly fail if there aren't enough nodes to meet the requirement
<<<<<<< .merge_file_a11928
        checkRequiredReads(nodes);

        final List<GetResult<R>> retrieved = Lists.newArrayList();

        // A count of the number of successful operations
        int successes = 0;
        // A list of thrown exceptions, indicating the number of failures
        final List<Throwable> failures = Lists.newArrayListWithCapacity(3);

        // Do the preferred number of reads in parallel
        int attempts = Math.min(this.storeDef.getPreferredReads(), nodes.size());
        int nodeIndex = 0;
        List<Callable<GetResult<R>>> callables = Lists.newArrayListWithCapacity(attempts);
        for(; nodeIndex < attempts; nodeIndex++) {
            final Node node = nodes.get(nodeIndex);
            callables.add(new GetCallable<R>(node, key, fetcher));
        }

        List<Future<GetResult<R>>> futures;
        try {
            futures = executor.invokeAll(callables, timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            throw new InsufficientOperationalNodesException("Get operation interrupted!", e);
        }

        for(Future<GetResult<R>> f: futures) {
            if(f.isCancelled()) {
                logger.warn("Get operation timed out after " + timeoutMs + " ms.");
                continue;
            }
            try {
                GetResult<R> getResult = f.get();
                if(getResult.exception != null) {
                    if(getResult.exception instanceof VoldemortApplicationException) {
                        throw (VoldemortException) getResult.exception;
                    }
                    failures.add(getResult.exception);
                    continue;
                }
                ++successes;
                retrieved.add(getResult);
            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException("Get operation interrupted!", e);
            } catch(ExecutionException e) {
                // We catch all Throwable subclasses apart from Error in the
                // callable, so the else
                // part should never happen.
                if(e.getCause() instanceof Error)
                    throw (Error) e.getCause();
                else
                    logger.error(e.getMessage(), e);
            }
        }

        // Now if we had any failures we will be short a few reads. Do serial
        // reads to make up for these.
        while(successes < this.storeDef.getPreferredReads() && nodeIndex < nodes.size()) {
            Node node = nodes.get(nodeIndex);
            long startNs = System.nanoTime();
            try {
                retrieved.add(new GetResult<R>(node,
                                               key,
                                               fetcher.execute(innerStores.get(node.getId()), key),
                                               null));
                ++successes;
                recordSuccess(node, startNs);
            } catch(UnreachableStoreException e) {
                failures.add(e);
                recordException(node, startNs, e);
            } catch(VoldemortApplicationException e) {
                throw e;
            } catch(Exception e) {
                logger.warn("Error in GET on node " + node.getId() + "(" + node.getHost() + ")", e);
                failures.add(e);
            }
            nodeIndex++;
        }

        if(logger.isTraceEnabled())
            logger.trace("GET retrieved the following node values: " + formatNodeValues(retrieved));

        if(preReturnProcedure != null)
            preReturnProcedure.apply(retrieved);

        if(successes >= this.storeDef.getRequiredReads()) {
            List<R> result = Lists.newArrayListWithExpectedSize(retrieved.size());
            for(GetResult<R> getResult: retrieved)
                result.addAll(getResult.retrieved);
            return result;
        } else
            throw new InsufficientSuccessfulNodesException(this.storeDef.getRequiredReads()
                                                                   + " reads required, but "
                                                                   + successes + " succeeded.",
                                                           failures,
                                                           storeDef.getReplicationFactor(),
                                                           storeDef.getRequiredReads(),
                                                           successes);

    }

    private void fillRepairReadsValues(final List<NodeValue<ByteArray, byte[]>> nodeValues,
                                       final ByteArray key,
                                       Node node,
                                       List<Versioned<byte[]>> fetched) {
        if(repairReads) {
            if(fetched.size() == 0)
                nodeValues.add(nullValue(node, key));
            else {
                for(Versioned<byte[]> f: fetched)
                    nodeValues.add(new NodeValue<ByteArray, byte[]>(node.getId(), key, f));
            }
        }
    }

    private NodeValue<ByteArray, byte[]> nullValue(Node node, ByteArray key) {
        return new NodeValue<ByteArray, byte[]>(node.getId(), key, new Versioned<byte[]>(null));
    }

    private void repairReads(List<NodeValue<ByteArray, byte[]>> nodeValues) {
        if(!repairReads || nodeValues.size() <= 1 || storeDef.getPreferredReads() <= 1)
            return;

        final List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        /*
         * We clone after computing read repairs in the assumption that the
         * output will be smaller than the input. Note that we clone the
         * version, but not the key or value as the latter two are not mutated.
         */
        for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
            Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                          ((VectorClock) v.getVersion()).clone());
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(), v.getKey(), versioned));
        }

        this.executor.execute(new Runnable() {

            public void run() {
                for(NodeValue<ByteArray, byte[]> v: toReadRepair) {
                    try {
                        System.out.println("Doing read repair on node " + v.getNodeId()
                                           + " for key '" + v.getKey() + "' with version "
                                           + v.getVersion() + ":"
                                           + new String(v.getVersioned().getValue()));
                        if(logger.isDebugEnabled())
                            logger.debug("Doing read repair on node " + v.getNodeId()
                                         + " for key '" + v.getKey() + "' with version "
                                         + v.getVersion() + ".");
                        innerStores.get(v.getNodeId()).put(v.getKey(), v.getVersioned());
                        System.out.println("Completed read repair on node " + v.getNodeId());
                    } catch(VoldemortApplicationException e) {
                        if(logger.isDebugEnabled())
                            logger.debug("Read repair cancelled due to application level exception on node "
                                         + v.getNodeId()
                                         + " for key '"
                                         + v.getKey()
                                         + "' with version "
                                         + v.getVersion()
                                         + ": "
                                         + e.getMessage());
                    } catch(Exception e) {
                        logger.debug("Read repair failed: ", e);
                    }
                }
            }
        });
    }

    private void checkRequiredReads(final List<Node> nodes)
            throws InsufficientOperationalNodesException {
        if(nodes.size() < this.storeDef.getRequiredReads())
            throw new InsufficientOperationalNodesException("Only "
                                                                    + nodes.size()
                                                                    + " nodes in preference list, but "
                                                                    + this.storeDef.getRequiredReads()
                                                                    + " reads required.",
                                                            nodes.size(),
                                                            storeDef.getRequiredReads());
    }

    private <R> String formatNodeValues(List<GetResult<R>> results) {
        // log all retrieved values
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(GetResult<?> r: results) {
            builder.append("GetResult(nodeId=" + r.node.getId() + ", key=" + r.key
                           + ", retrieved= " + r.retrieved + ")");
            builder.append(", ");
        }
        builder.append("}");

        return builder.toString();
    }

    public String getName() {
        return this.name;
=======
        checkRequiredReads(nodes.size());
        return super.getVersions(key, nodes);
>>>>>>> .merge_file_a15396
    }

    /**
     * Update a key valu ensuring required writes. It ensures that it first
     * succeeds on one node before firing off to other nodes to meet required
     * writes criteria.
     * 
     * @param key The key to use
     * @param versioned The versioned value.
     * @return The Version after update.
     * @throws VoldemortException
     */
    @Override
    public Version put(final ByteArray key, final Versioned<byte[]> versioned)
            throws VoldemortException {

        StoreUtils.assertValidKey(key);
        final List<Integer> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
        // quickly fail if there aren't enough nodes to meet the requirement
        checkRequiredWrites(nodes);

        // If requiredWrites > 0 then do a single blocking write to the first
        // live node in the preference list if this node throws an
        // ObsoleteVersionException allow it to propagate
        int master = -1;
        Versioned<byte[]> versionedCopy = null;
        int numNodes = nodes.size();
        int currentNode = 0;
        long startNs = System.nanoTime();
        Version retVersion = null;
        for(; currentNode < numNodes; currentNode++) {
            checkRequiredWrites(numNodes - currentNode);
            try {
                int current = nodes.get(currentNode);
                versionedCopy = incremented(versioned, current);
                Store<ByteArray, byte[]> store = this.getNodeStore(current);
                retVersion = store.put(key, versionedCopy);
                master = currentNode;
                break;
            } catch(UnreachableStoreException e) {
                if(logger.isDebugEnabled()) {
                    logger.debug("impossible to reach the node - It's marked unavailable.");
                }
            } catch(ObsoleteVersionException e) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Obsolete version for key: " + key);
                }
                // if this version is obsolete on the master, then bail out
                // of this operation
                throw e;
            } catch(Exception e) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Exception found during PUT in master node Id: " + currentNode
                                 + " - " + e.getMessage(), e);
                }
            }
        }

        if(master < 0) {
            if(logger.isDebugEnabled()) {
                logger.debug("Impossible to complete PUT from master node");
            }
            throw new InsufficientOperationalNodesException("No master node succeeded!");
        }

        final List<Integer> replicas = new ArrayList<Integer>(numNodes);
        for(currentNode = master + 1; currentNode < numNodes; currentNode++) {
            replicas.add(nodes.get(currentNode));
        }

        try {
            long elapsed = System.nanoTime() - startNs;
            long remaining = timeout - timeUnit.convert(elapsed, TimeUnit.NANOSECONDS);
            if(replicas.size() > 0) {
                retVersion = super.put(key,
                                           versionedCopy,
                                           replicas,
                                           this.preferredWrites - 1,
                                           this.requiredWrites - 1,
                                           remaining,
                                           timeUnit);
            }
            // Okay looks like it worked, increment the version for the caller
            // This is just for test cases failure prevention
            // TODO: Remove this
            VectorClock versionedClock = (VectorClock) versioned.getVersion();
            versionedClock.incrementClock(master, time.getMilliseconds());

            if(logger.isDebugEnabled()) {
                logger.debug("successfully terminated PUT based on quorum requirements.");
            }
            return retVersion;
        } catch(InsufficientSuccessfulNodesException e) {
            // If we got an insufficient, update the number
            // success/required/failed to account for the master
            e.setAvailable(e.getAvailable() + 1);
            e.setRequired(e.getRequired() + 1);
            e.setSuccessful(e.getSuccessful() + 1);
            throw e;
        }
    }

    /**
     * Increments version when for a versioned object.
     * 
     * @param versioned The old versioned object
     * @param nodeId The node where the write was mastered.
     * @return the incremented versioned value.
     */
    private Versioned<byte[]> incremented(Versioned<byte[]> versioned, int nodeId) {
        Version incremented = VersionFactory.cloneVersion(versioned.getVersion());
        incremented.incrementClock(nodeId, time.getMilliseconds());

        return new Versioned<byte[]>(versioned.getValue(), incremented, versioned.getMetadata());
    }

    public void updateRoutingStrategy(RoutingStrategy routingStrategy) {
        logger.info("Updating routing strategy for RoutedStore:" + getName());
        this.routingStrategy = routingStrategy;
    }

    /**
     * Delete keys ensuring deletion from minimum required writes nodes.
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return true on successful deletion.
     * 
     * @throws VoldemortException on failure to delete from one or many nodes.
     */
    @Override
    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Integer> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
        this.checkRequiredWrites(nodes);
        return super.delete(key, version, nodes);
    }

    /**
     * Get a key from servers. Looks for at least minimum required reads for
     * success. Triggers read repair if values are out of date, even on
     * failures.
     * 
     * @param key The key to check for
     * @return the list of returned versioned values.
     */
    @Override
    public List<Versioned<byte[]>> get(ByteArray key) {
        StoreUtils.assertValidKey(key);
        final List<Integer> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
        // quickly fail if there aren't enough nodes to meet the requirement
        checkRequiredReads(nodes);
        return super.get(key, nodes);
    }

    /**
     * Get values for a list of keys.
     * 
     * @param keys The keys to check for.
     * @return The list of versioned objects.
     * 
     * @throws VoldemortException on failure to get minimum number of reads.
     */
    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        // Keys for each node needed to satisfy storeDef.getPreferredReads() if
        // no failures.
        Map<Integer, List<ByteArray>> nodeToKeysMap = Maps.newHashMap();

        for(ByteArray key: keys) {
            List<Integer> availableNodes = availableNodes(routingStrategy.routeRequest(key.get()));
            // quickly fail if there aren't enough nodes to meet the requirement
            checkRequiredReads(availableNodes);

            for(Integer nodeId: availableNodes) {
                List<ByteArray> nodeKeys = nodeToKeysMap.get(nodeId);
                if(nodeKeys == null) {
                    nodeKeys = Lists.newArrayList();
                    nodeToKeysMap.put(nodeId, nodeKeys);
                }
                nodeKeys.add(key);
            }
        }

        return super.getAll(nodeToKeysMap);
    }

    /**
     * @param list A list of nodes to check in.
     * 
     * @return All the nodes which are available among the input nodes list.
     */
    protected List<Integer> availableNodes(List<Node> list) {
        List<Integer> available = new ArrayList<Integer>(list.size());
        for(Node node: list)
            if(isAvailable(node))
                available.add(node.getId());
        return available;
    }

    /**
     * Check if a node is available.
     * 
     * @param node The node being checked.
     * @return true if available, false if it is under suspension (due to
     *         previous failure and node bannage is not over).
     */
    boolean isAvailable(Node node) {
        return failureDetector.isAvailable(node);
    }

}
