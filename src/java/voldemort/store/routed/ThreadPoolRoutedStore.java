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

package voldemort.store.routed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortApplicationException;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public class ThreadPoolRoutedStore extends RoutedStore {

    private final FailureDetector failureDetector;

    /**
     * Create a RoutedStoreClient
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param routingStrategy The strategy for choosing a node given a key
     * @param requiredReads The minimum number of reads that must complete
     *        before the operation will return
     * @param requiredWrites The minimum number of writes that must complete
     *        before the operation will return
     * @param numberOfThreads The number of threads in the threadpool
     */
    public ThreadPoolRoutedStore(String name,
                                 DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                                 Cluster cluster,
                                 StoreDefinition storeDef,
                                 FailureDetector failureDetector,
                                 long timeoutMs,
                                 Time time) {
        super(name, distributor, cluster, storeDef, timeoutMs, time);
        this.failureDetector = failureDetector;
    }

    protected <R> R waitForCompletion(StoreFuture<R> future) {
        return waitForCompletion(future, timeoutMs, TimeUnit.MILLISECONDS);
    }

    protected <R> R waitForCompletion(StoreFuture<R> future, long timeout, TimeUnit units) {
        try {
            R result = future.get(timeout, units);
            return result;
        } catch(VoldemortException e) {
            throw e;

        }
    }

    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
        checkRequiredWrites(nodes.size());
        return waitForCompletion(distributor.submitDelete(key,
                                                          version,
                                                          nodes,
                                                          storeDef.getPreferredWrites(),
                                                          storeDef.getRequiredWrites()));
    }

    protected Map<Node, List<ByteArray>> getNodesForKeys(Iterable<ByteArray> keys)
            throws VoldemortException {
        Map<Node, List<ByteArray>> nodeToKeys = Maps.newHashMap();
        for(ByteArray key: keys) {
            List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
            checkRequiredReads(nodes.size());
            for(Node node: nodes) {
                List<ByteArray> keysForNode = nodeToKeys.get(node);
                if(keysForNode == null) {
                    keysForNode = new ArrayList<ByteArray>();
                    nodeToKeys.put(node, keysForNode);
                }
                keysForNode.add(key);
            }
        }
        return nodeToKeys;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<Node, List<ByteArray>> nodesForKeys = getNodesForKeys(keys);
        return waitForCompletion(distributor.submitGetAll(nodesForKeys,
                                                          transforms,
                                                          storeDef.getPreferredReads(),
                                                          storeDef.getRequiredReads()));
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transform) {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
        checkRequiredReads(nodes.size());

        return waitForCompletion(distributor.submitGet(key,
                                                       transform,
                                                       nodes,
                                                       storeDef.getPreferredReads(),
                                                       storeDef.getRequiredReads()));
    }

    public Version put(final ByteArray key,
                       final Versioned<byte[]> versioned,
                       final byte[] transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));
        // quickly fail if there aren't enough nodes to meet the requirement

        // If requiredWrites > 0 then do a single blocking write to the first
        // live node in the preference list if this node throws an
        // ObsoleteVersionException allow it to propagate
        int master = -1;
        Versioned<byte[]> versionedCopy = null;
        int numNodes = nodes.size();
        int currentNode = 0;
        long startNs = System.nanoTime();
        // A list of thrown exceptions, indicating the number of failures
        final List<Exception> failures = Lists.newArrayList();
        Version retVersion = null;
        for(; currentNode < numNodes; currentNode++) {
            checkRequiredWrites(numNodes - currentNode);
            try {
                Node current = nodes.get(currentNode);
                versionedCopy = incremented(versioned, current.getId());
                AsynchronousStore<ByteArray, byte[], byte[]> async = distributor.getNodeStore(current);
                retVersion = waitForCompletion(async.submitPut(key, versionedCopy, transform));
                master = currentNode;
                break;
            } catch(UnreachableStoreException e) {
                // logger.warn("impossible to reach the node - It's marked unavailable - "
                // + e.getMessage(), e);
                failures.add(e);
            } catch(ObsoleteVersionException e) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Obsolete version for key: " + key);
                }
                // if this version is obsolete on the master, then bail out
                // of this operation
                throw e;
            } catch(VoldemortApplicationException e) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Caught application exception: " + e.getMessage());
                }
                // Bail out of this operation
                throw e;
            } catch(Exception e) {
                logger.warn("Exception found during PUT in master node Id: "
                            + nodes.get(currentNode) + " - " + e.getMessage() /*
                                                                               * ,
                                                                               * e
                                                                               */);
                failures.add(e);
            }
        }

        if(master < 0) {
            if(logger.isDebugEnabled()) {
                logger.debug("Impossible to complete PUT from master node");
            }
            throw new InsufficientOperationalNodesException("No master node succeeded!",
                                                            failures,
                                                            0,
                                                            storeDef.getRequiredWrites());
        }

        final List<Node> replicas = new ArrayList<Node>(numNodes);
        for(currentNode = master + 1; currentNode < numNodes; currentNode++) {
            replicas.add(nodes.get(currentNode));
        }

        long elapsed = System.nanoTime() - startNs;
        // changing timeout behavior to provide enough time for replica writes
        // long remaining = timeout - timeUnit.convert(elapsed,
        // TimeUnit.NANOSECONDS);
        long remaining = timeoutMs;
        if(replicas.size() > 0) {
            try {
                waitForCompletion(distributor.submitPut(key,
                                                        versionedCopy,
                                                        transform,
                                                        replicas,
                                                        storeDef.getPreferredWrites() - 1,
                                                        storeDef.getRequiredWrites() - 1));
            } catch(InsufficientSuccessfulNodesException e) {
                e.setSuccessful(e.getSuccessful() + 1);
                e.setAvailable(nodes.size());
                e.setRequired(storeDef.getRequiredWrites());
                throw e;
            }
        }

        if(logger.isDebugEnabled()) {
            logger.debug("successfully terminated PUT based on quorum requirements.");
        }
        return retVersion;
    }

    private Versioned<byte[]> incremented(Versioned<byte[]> versioned, int nodeId) {
        Version incremented = VersionFactory.cloneVersion(versioned.getVersion());
        incremented.incrementClock(nodeId, time.getMilliseconds());

        return new Versioned<byte[]>(versioned.getValue(), incremented, versioned.getMetadata());
    }

    private List<Node> availableNodes(List<Node> list) {
        List<Node> available = new ArrayList<Node>(list.size());
        for(Node node: list)
            if(failureDetector.isAvailable(node))
                available.add(node);
        return available;
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));

        return waitForCompletion(distributor.submitGetVersions(key,
                                                               nodes,
                                                               storeDef.getPreferredReads(),
                                                               storeDef.getRequiredReads()));
    }
}
