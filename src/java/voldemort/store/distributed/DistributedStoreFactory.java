/*
 * Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.store.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.client.StoreFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.routed.ReadRepairStore;
import voldemort.store.slop.HintedHandoffStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * Utility/factory class for distributed store operations.
 * */
public class DistributedStoreFactory {

    /**
     * Returns the distributed store as an asynchronous one.
     * 
     * @param storeDef The definition for the store.
     * @param distributor The store to convert
     * @return The asynchronous representation of the distributed store.
     */
    public static <N, K, V, T> AsynchronousStore<K, V, T> asAsync(StoreDefinition storeDef,
                                                                  DistributedStore<N, K, V, T> distributor) {
        return new AsynchronousDistributedStore<N, K, V, T>(storeDef, distributor);
    }

    public static DistributedStore<Node, ByteArray, byte[], byte[]> create(StoreFactory factory,
                                                                           StoreDefinition storeDef,
                                                                           Cluster cluster,
                                                                           FailureDetector detector,
                                                                           int zoneId) {
        return create(factory, storeDef, cluster, detector, zoneId, false);
    }

    public static DistributedStore<Node, ByteArray, byte[], byte[]> create(StoreFactory factory,
                                                                           StoreDefinition storeDef,
                                                                           Cluster cluster,
                                                                           FailureDetector detector,
                                                                           int zoneId,
                                                                           boolean makeUnique) {
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> nodeStores = factory.getNodeStores(storeDef);
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = create(nodeStores,
                                                                               storeDef,
                                                                               cluster,
                                                                               zoneId,
                                                                               makeUnique);

        if(storeDef.hasHintedHandoffStrategyType()) {
            Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> slopStores = Maps.newHashMap();
            for(Node node: cluster.getNodes()) {
                AsynchronousStore<ByteArray, byte[], byte[]> async = factory.getNodeStore(node,
                                                                                          "slop");
                slopStores.put(node, async);
            }
            distributor = new HintedHandoffStore(distributor,
                                                 storeDef,
                                                 cluster,
                                                 slopStores,
                                                 detector,
                                                 zoneId);
        }
        return distributor;
    }

    public static DistributedStore<Node, ByteArray, byte[], byte[]> create(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> nodeStores,
                                                                           StoreDefinition storeDef,
                                                                           Cluster cluster,
                                                                           int zoneId) {
        return create(nodeStores, storeDef, cluster, zoneId, false);
    }

    public static DistributedStore<Node, ByteArray, byte[], byte[]> create(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> nodeStores,
                                                                           StoreDefinition storeDef,
                                                                           Cluster cluster,
                                                                           int zoneId,
                                                                           boolean makeUnique) {
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = DistributedParallelStore.create(nodeStores,
                                                                                                        storeDef,
                                                                                                        makeUnique);

        if(storeDef.isZoneRoutingEnabled()) {
            distributor = DistributedZoneStore.create(distributor,
                                                      storeDef,
                                                      cluster,
                                                      zoneId,
                                                      makeUnique);
        }

        if(!storeDef.isView()) {
            distributor = ReadRepairStore.create(distributor);
        }
        return distributor;
    }

    public static DistributedStore<Node, ByteArray, byte[], byte[]> create(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> nodeStores,
                                                                           StoreDefinition storeDef,
                                                                           boolean makeUnique) {
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = DistributedParallelStore.create(nodeStores,
                                                                                                        storeDef,
                                                                                                        makeUnique);
        if(!storeDef.isView()) {
            distributor = ReadRepairStore.create(distributor);
        }
        return distributor;
    }

    public static void checkRequiredReads(int nodes, int required)
            throws InsufficientOperationalNodesException {
        if(nodes < required)
            throw new InsufficientOperationalNodesException("Only "
                                                                    + nodes
                                                                    + " nodes in preference list, but "
                                                                    + required + " reads required",
                                                            nodes,
                                                            required);
    }

    public static void checkRequiredZones(int zones, int required)
            throws InsufficientOperationalNodesException {
        if(zones < required)
            throw new InsufficientOperationalNodesException("Only " + zones + " available, but "
                                                                    + required + " are required",
                                                            zones,
                                                            required);
    }

    public static void checkRequiredWrites(int nodes, int required)
            throws InsufficientOperationalNodesException {
        if(nodes < required)
            throw new InsufficientOperationalNodesException("Only "
                                                                    + nodes
                                                                    + " nodes in preference list, but "
                                                                    + required + " writes required",
                                                            nodes,
                                                            required);
    }

    public static <N> ResultsBuilder<N, Version> PutBuilder() {
        return new ResultsBuilder<N, Version>() {

            public Version buildResponse(Map<N, Version> responses) throws VoldemortException {
                Version version = null;
                for(Version response: responses.values()) {
                    if(version == null) {
                        version = response;
                    } else if(!version.equals(response)) {
                        // What do we do here? Ignore for now
                    }
                }
                return version;
            }
        };
    }

    public static <N> ResultsBuilder<N, Boolean> DeleteBuilder() {
        return new ResultsBuilder<N, Boolean>() {

            public Boolean buildResponse(Map<N, Boolean> responses) throws VoldemortException {
                for(Boolean response: responses.values()) {
                    if(response.booleanValue()) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static <V> List<Versioned<V>> collapse(Collection<List<Versioned<V>>> responses,
                                                  boolean unique) {
        List<Versioned<V>> results = new ArrayList<Versioned<V>>();
        for(List<Versioned<V>> response: responses) {
            results.addAll(response);
        }
        if(unique) {
            results = VectorClockInconsistencyResolver.ResolveConflicts(results);
        }
        return results;
    }

    public static <N, V> ResultsBuilder<N, List<Versioned<V>>> GetBuilder(final boolean makeUnique) {
        return new ResultsBuilder<N, List<Versioned<V>>>() {

            public List<Versioned<V>> buildResponse(Map<N, List<Versioned<V>>> responses)
                    throws VoldemortException {
                return collapse(responses.values(), makeUnique);
            }
        };
    }

    public static <N, K, V> Map<N, List<Versioned<V>>> getKeyByNode(K key,
                                                                    Map<N, Map<K, List<Versioned<V>>>> responses,
                                                                    Map<N, List<K>> nodesToKeys) {
        Map<N, List<Versioned<V>>> results = getKeyByNode(key, responses);
        for(Map.Entry<N, List<K>> entry: nodesToKeys.entrySet()) {
            N node = entry.getKey();
            // If the results do not already contain the node for the key
            if(!results.containsKey(node)) {
                // If the node was supposed to respond to the key
                if(entry.getValue().contains(key)) {
                    // Add an empty entry
                    results.put(node, new ArrayList<Versioned<V>>());
                }
            }
        }
        return results;

    }

    public static <N, K, V> Map<N, List<Versioned<V>>> getKeyByNode(K key,
                                                                    Map<N, Map<K, List<Versioned<V>>>> responses) {
        Map<N, List<Versioned<V>>> results = Maps.newHashMap();

        for(Map.Entry<N, Map<K, List<Versioned<V>>>> entry: responses.entrySet()) {
            if(entry.getValue().containsKey(key)) {
                results.put(entry.getKey(), entry.getValue().get(key));
            }
        }
        return results;
    }

    public static <N, K, V> ResultsBuilder<N, Map<K, List<Versioned<V>>>> GetAllBuilder(final Map<N, List<K>> nodesToKeys,
                                                                                        final int required,
                                                                                        final boolean makeUnique) {
        return new ResultsBuilder<N, Map<K, List<Versioned<V>>>>() {

            public Map<K, List<Versioned<V>>> buildResponse(Map<N, Map<K, List<Versioned<V>>>> responses)
                    throws VoldemortException {
                Set<K> visited = new HashSet<K>();
                Map<K, List<Versioned<V>>> results = Maps.newHashMap();
                for(Map.Entry<N, Map<K, List<Versioned<V>>>> entry: responses.entrySet()) {
                    for(K key: entry.getValue().keySet()) {
                        if(!visited.contains(key)) {
                            visited.add(key);
                            Map<N, List<Versioned<V>>> responsesForKey = getKeyByNode(key,
                                                                                      responses,
                                                                                      nodesToKeys);
                            if(responsesForKey.size() >= required) {
                                List<Versioned<V>> result = collapse(responsesForKey.values(),
                                                                     makeUnique);
                                if(result.size() > 0) {
                                    results.put(key, result);
                                }
                            }
                        }
                    }
                }
                return results;
            }
        };
    }

    public static <N> void assertValidNodes(Collection<N> nodes,
                                            Collection<N> available,
                                            int preferred,
                                            int required) {
        if(nodes == null) {
            throw new IllegalArgumentException("Nodes cannot be null.");
        } else if(required > preferred) {
            throw new IllegalArgumentException("Required must be at least preferred.");
        } else if(required < 0) {
            throw new IllegalArgumentException("Required must be greater than or equal to zero.");
        }
        for(N node: nodes) {
            if(!available.contains(node)) {
                throw new IllegalArgumentException("Node " + node + " is not allowed");
            }
        }
    }

    public static <N, V> ResultsBuilder<N, List<Version>> GetVersionsBuilder(final boolean makeUnique) {
        return new ResultsBuilder<N, List<Version>>() {

            public List<Version> buildResponse(Map<N, List<Version>> responses)
                    throws VoldemortException {
                List<Version> results = new ArrayList<Version>();
                for(List<Version> response: responses.values()) {
                    results.addAll(response);
                }
                if(makeUnique) {
                    results = VectorClockInconsistencyResolver.getVersions(results);
                }
                return results;
            }
        };
    }

}
