/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright 2010 Nokia Corporation. All rights reserved.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.VoldemortException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;
import voldemort.store.distributed.DelegatingDistributedStore;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * A distributed store that repairs out of date keys returned by read/get
 * operations. This store listens for the get operations to complete and then
 * repairs any keys that were missing or had obsolete values.
 */
public class ReadRepairStore<N, K, V, T> extends DelegatingDistributedStore<N, K, V, T> {

    private final ReadRepairer<N, K, V> readRepairer;
    /**
     * The number of read repair operations that have been submitted but not
     * completed.
     */
    final AtomicInteger pendingRepairs;

    public static <N, K, V, T> ReadRepairStore<N, K, V, T> create(DistributedStore<N, K, V, T> distributor) {
        return new ReadRepairStore<N, K, V, T>(distributor);
    }

    /**
     * Creates a read repair store from the input store
     * 
     * @param distributor The store to read repair.
     */
    public ReadRepairStore(DistributedStore<N, K, V, T> distributor) {
        super(distributor);
        this.pendingRepairs = new AtomicInteger(0);
        this.readRepairer = new ReadRepairer<N, K, V>();
    }

    /**
     * Get the value associated with the given key and issues any necessary read
     * repairs.
     * 
     * @param key The key to check for
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    @Override
    public DistributedFuture<N, List<Versioned<V>>> submitGet(final K key,
                                                              final T transform,
                                                              Collection<N> nodes,
                                                              int preferred,
                                                              int required)
            throws VoldemortException {
        final DistributedFuture<N, List<Versioned<V>>> future = super.submitGet(key,
                                                                                transform,
                                                                                nodes,
                                                                                preferred,
                                                                                required);
        StoreFutureListener<List<Versioned<V>>> listener = new StoreFutureListener<List<Versioned<V>>>() {

            public void futureCompleted(Object arg, List<Versioned<V>> result, long duration) {
                Map<N, List<Versioned<V>>> results = future.getResults();
                List<NodeValue<N, K, V>> repairs = buildRepairReads(key, results);
                if(repairs.size() > 0) {
                    repairReads(repairs);
                }
            }

            public void futureFailed(Object arg, VoldemortException ex, long duration) {
                Map<N, List<Versioned<V>>> results = future.getResults();
                List<NodeValue<N, K, V>> repairs = buildRepairReads(key, results);
                if(repairs.size() > 0) {
                    repairReads(repairs);
                }
            }
        };

        future.register(listener, future);
        return future;
    }

    /**
     * Get the values associated with the given keys and issues any necessary
     * read repairs.
     * 
     * @param nodesToKeys The keys to check for
     * @return The values associated with the keys
     * @throws VoldemortException
     */
    @Override
    public DistributedFuture<N, Map<K, List<Versioned<V>>>> submitGetAll(final Map<N, List<K>> nodesToKeys,
                                                                         final Map<K, T> transforms,
                                                                         int preferred,
                                                                         int required)
            throws VoldemortException {
        final DistributedFuture<N, Map<K, List<Versioned<V>>>> future = super.submitGetAll(nodesToKeys,
                                                                                           transforms,
                                                                                           preferred,
                                                                                           required);
        StoreFutureListener<Map<K, List<Versioned<V>>>> listener = new StoreFutureListener<Map<K, List<Versioned<V>>>>() {

            public void futureCompleted(Object arg, Map<K, List<Versioned<V>>> result, long duration) {
                Map<N, Map<K, List<Versioned<V>>>> results = future.getResults();
                List<NodeValue<N, K, V>> repairs = buildRepairReads(nodesToKeys, results);
                if(repairs.size() > 0) {
                    repairReads(repairs);
                }
            }

            public void futureFailed(Object arg, VoldemortException ex, long duration) {
                Map<N, Map<K, List<Versioned<V>>>> results = future.getResults();
                List<NodeValue<N, K, V>> repairs = buildRepairReads(nodesToKeys, results);
                if(repairs.size() > 0) {
                    repairReads(repairs);
                }
            }
        };
        future.register(listener, null);
        return future;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case READ_REPAIRER:
                return this.readRepairer;
            default:
                return super.getCapability(capability);
        }
    }

    /**
     * Returns a null valued NodeValue.
     * 
     * @param node The node.
     * @param key the key.
     * @return NodeValue with null versioned values.
     */
    private NodeValue<N, K, V> nullValue(N node, K key) {
        return new NodeValue<N, K, V>(node, key, new Versioned<V>(null));
    }

    /**
     * Fills nodeValues using provided parameters. In case fetch list is null,
     * it puts in a null entry so that read repair is triggered
     * 
     * @param nodeValues The list which is populated.
     * @param key The key
     * @param node The node where the key was obtained from.
     * @param fetched The versions obtained from the node.
     */
    private void fillRepairReadsValues(final List<NodeValue<N, K, V>> nodeValues,
                                       final K key,
                                       N node,
                                       List<Versioned<V>> fetched) {
        // fetched can be null when used from getAll
        if(fetched == null || fetched.size() == 0) {
            nodeValues.add(nullValue(node, key));
        } else {
            for(Versioned<V> f: fetched)
                nodeValues.add(new NodeValue<N, K, V>(node, key, f));
        }
    }

    protected List<NodeValue<N, K, V>> buildRepairReads(K key,
                                                        Map<N, List<Versioned<V>>> nodeResults) {
        if(logger.isDebugEnabled()) {
            logger.debug("Read repair for key: " + key.toString());
        }

        List<NodeValue<N, K, V>> nodeValues = Lists.newArrayListWithExpectedSize(nodeResults.size());
        for(Entry<N, List<Versioned<V>>> entry: nodeResults.entrySet()) {
            final N node = entry.getKey();
            fillRepairReadsValues(nodeValues, key, node, entry.getValue());
        }
        return nodeValues;

    }

    protected List<NodeValue<N, K, V>> buildRepairReads(Map<N, List<K>> keys,
                                                        Map<N, Map<K, List<Versioned<V>>>> values) {
        List<NodeValue<N, K, V>> nodeValues = Lists.newArrayListWithExpectedSize(values.size());

        for(Entry<N, List<K>> entry: keys.entrySet()) {
            N node = entry.getKey();
            Map<K, List<Versioned<V>>> result = values.get(node);
            if(result != null) {
                for(K key: entry.getValue()) {
                    fillRepairReadsValues(nodeValues, key, node, result.get(key));
                }
            }
        }
        return nodeValues;
    }

    /**
     * Conducts read repairs if needed. The read repairs are conducted by a
     * non-blocking thread.
     * 
     * @param nodeValues The list of node-value mappings as received in get() or
     *        getAll()
     */
    private void repairReads(final List<NodeValue<N, K, V>> nodeValues) {
        final List<NodeValue<N, K, V>> pendingRepairs = Lists.newArrayList();
        /*
         * We clone after computing read repairs in the assumption that the
         * output will be smaller than the input. Note that we clone the
         * version, but not the key or value as the latter two are not mutated.
         */
        Collection<NodeValue<N, K, V>> repairs = readRepairer.getRepairs(nodeValues);
        for(NodeValue<N, K, V> v: repairs) {
            Versioned<V> cloned = v.getVersioned().cloneVersioned();
            pendingRepairs.add(new NodeValue<N, K, V>(v.getNode(), v.getKey(), cloned));
        }
        processRepairs(pendingRepairs);
    }

    private void processRepairs(List<NodeValue<N, K, V>> pendingRepairs) {
        this.pendingRepairs.addAndGet(pendingRepairs.size());
        for(final NodeValue<N, K, V> repair: pendingRepairs) {
            startRepair(repair);
        }
    }

    private StoreFuture<Version> startRepair(final NodeValue<N, K, V> repair) {
        if(logger.isDebugEnabled())
            logger.debug("Starting read repair on node " + repair.getNode() + " for key '"
                         + repair.getKey() + "' with version " + repair.getVersion() + ".");
        final AsynchronousStore<K, V, T> store = getNodeStore(repair.getNode());
        StoreFuture<Version> future = store.submitPut(repair.getKey(), repair.getVersioned(), null);
        future.register(new StoreFutureListener<Version>() {

            public void futureCompleted(Object arg, Version version, long duration) {
                pendingRepairs.decrementAndGet();
                if(logger.isDebugEnabled())
                    logger.debug("Repaired key '" + repair.getKey() + "' `on node "
                                 + repair.getNode() + " to version " + version);
            }

            public void futureFailed(Object arg, VoldemortException ex, long duration) {
                pendingRepairs.decrementAndGet();
                if(ex instanceof ObsoleteVersionException) {
                    if(logger.isDebugEnabled())
                        logger.debug("Read repair cancelled due to obsolete version on node "
                                     + repair.getNode() + " for key '" + repair.getKey()
                                     + "' with version " + repair.getVersion() + ": "
                                     + ex.getMessage());
                } else {
                    logger.debug("Read repair failed: ", ex);
                }
            }
        }, repair.getKey());
        return future;
    }
}
