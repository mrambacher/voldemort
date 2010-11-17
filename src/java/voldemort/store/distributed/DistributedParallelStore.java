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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * A distributed store that runs requests in parallel and waits for the
 * preferred/required nodes to complete.
 */
public class DistributedParallelStore<N, K, V> implements DistributedStore<N, K, V> {

    private final boolean makeUnique;
    /** The map of nodes to asynchronous stores */
    final Map<N, AsynchronousStore<K, V>> nodeStores;
    protected final ResultsBuilder<N, List<Versioned<V>>> getBuilder;
    protected final ResultsBuilder<N, List<Version>> versionsBuilder;
    protected final ResultsBuilder<N, Version> putBuilder;
    protected final ResultsBuilder<N, Boolean> deleteBuilder;
    protected final Logger logger = LogManager.getLogger(getClass());

    private final StoreDefinition storeDef;

    public DistributedParallelStore(Map<N, AsynchronousStore<K, V>> stores,
                                    StoreDefinition storeDef,
                                    boolean unique) {
        this.storeDef = storeDef;
        this.nodeStores = stores;
        this.makeUnique = unique;
        this.getBuilder = DistributedStoreFactory.GetBuilder(unique);
        this.versionsBuilder = DistributedStoreFactory.GetVersionsBuilder(unique);
        this.putBuilder = DistributedStoreFactory.PutBuilder();
        this.deleteBuilder = DistributedStoreFactory.DeleteBuilder();
    }

    /**
     * Returns the nodes for distribution for the input key
     * 
     * @param key The key for the request
     * @return All of the nodes.
     */
    public Collection<N> getNodesForKey(final K key) {
        Map<N, AsynchronousStore<K, V>> stores = getNodeStores();
        return stores.keySet();
    }

    protected <R> DistributedFuture<N, R> submit(AsynchronousStore.Operations operation,
                                                 Map<N, StoreFuture<R>> futures,
                                                 ResultsBuilder<N, R> builder,
                                                 int preferred,
                                                 int required) {
        return new DistributedFutureTask<N, R>(operation.name(),
                                               futures,
                                               builder,
                                               preferred,
                                               required);
    }

    public AsynchronousStore<K, V> getNodeStore(N node) {
        return nodeStores.get(node);
    }

    public Map<N, AsynchronousStore<K, V>> getNodeStores() {
        return nodeStores;
    }

    public DistributedFuture<N, List<Versioned<V>>> submitGet(final K key,
                                                              Collection<N> nodes,
                                                              int preferred,
                                                              int required)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredReads(nodes, required);

        Map<N, StoreFuture<List<Versioned<V>>>> futures = Maps.newHashMap();
        for(N node: nodes) {
            AsynchronousStore<K, V> async = nodeStores.get(node);
            futures.put(node, async.submitGet(key));
        }
        return submit(AsynchronousStore.Operations.GET, futures, getBuilder, preferred, required);
    }

    public DistributedFuture<N, Map<K, List<Versioned<V>>>> submitGetAll(final Map<N, List<K>> nodesToKeys,
                                                                         int preferred,
                                                                         int required)
            throws VoldemortException {
        if(nodesToKeys == null) {
            throw new IllegalArgumentException("Nodes cannot be null.");
        }
        DistributedStoreFactory.assertValidNodes(nodesToKeys.keySet(),
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);

        Map<N, StoreFuture<Map<K, List<Versioned<V>>>>> futures = Maps.newHashMap();
        for(Map.Entry<N, List<K>> entry: nodesToKeys.entrySet()) {
            AsynchronousStore<K, V> async = nodeStores.get(entry.getKey());
            futures.put(entry.getKey(), async.submitGetAll(entry.getValue()));
        }
        ResultsBuilder<N, Map<K, List<Versioned<V>>>> getAllBuilder = DistributedStoreFactory.GetAllBuilder(nodesToKeys,
                                                                                                            required,
                                                                                                            makeUnique);
        return submit(AsynchronousStore.Operations.GETALL,
                      futures,
                      getAllBuilder,
                      nodesToKeys.size(),
                      1);
    }

    public DistributedFuture<N, Version> submitPut(K key,
                                                   Versioned<V> value,
                                                   Collection<N> nodes,
                                                   int preferred,
                                                   int required) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredWrites(nodes, required);
        Map<N, StoreFuture<Version>> futures = Maps.newHashMap();
        for(N node: nodes) {
            AsynchronousStore<K, V> async = nodeStores.get(node);
            futures.put(node, async.submitPut(key, value));
        }
        return submit(AsynchronousStore.Operations.PUT,
                      futures,
                      this.putBuilder,
                      preferred,
                      required);
    }

    protected void checkRequiredReads(final Collection<N> nodes, int required)
            throws InsufficientOperationalNodesException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
    }

    protected void checkRequiredWrites(final Collection<N> nodes, int required)
            throws InsufficientOperationalNodesException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
    }

    public DistributedFuture<N, Boolean> submitDelete(K key,
                                                      Version version,
                                                      Collection<N> nodes,
                                                      int preferred,
                                                      int required) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredWrites(nodes, required);
        Map<N, StoreFuture<Boolean>> futures = Maps.newHashMap();
        for(N node: nodes) {
            AsynchronousStore<K, V> async = nodeStores.get(node);
            futures.put(node, async.submitDelete(key, version));
        }
        return submit(AsynchronousStore.Operations.DELETE,
                      futures,
                      this.deleteBuilder,
                      preferred,
                      required);
    }

    public DistributedFuture<N, List<Version>> submitGetVersions(K key,
                                                                 Collection<N> nodes,
                                                                 int preferred,
                                                                 int required)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredReads(nodes, required);
        Map<N, StoreFuture<List<Version>>> futures = Maps.newHashMap();
        for(N node: nodes) {
            AsynchronousStore<K, V> async = nodeStores.get(node);
            futures.put(node, async.submitGetVersions(key));
        }
        return submit(AsynchronousStore.Operations.GETVERSIONS,
                      futures,
                      this.versionsBuilder,
                      preferred,
                      required);
    }

    /**
     * @return The name of the store.
     */
    public String getName() {
        return storeDef.getName();
    }

    /**
     * Close the store.
     * 
     * @throws VoldemortException If closing fails.
     */
    public void close() throws VoldemortException {
        VoldemortException exception = null;
        for(AsynchronousStore<K, V> async: nodeStores.values()) {
            try {
                async.close();
            } catch(VoldemortException e) {
                exception = e;
            }
        }
        if(exception != null) {
            throw exception;
        }
    }

    /**
     * Get some capability of the store. Examples would be the serializer used,
     * or the routing strategy. This provides a mechanism to verify that the
     * store hierarchy has some set of capabilities without knowing the precise
     * layering.
     * 
     * @param capability The capability type to retrieve
     * @return The given capaiblity
     * @throws NoSuchCapabilityException if the capaibility is not present
     */
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case NODE_STORES:
                return this.nodeStores;
            case ASYNCHRONOUS:
                return this;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }
}
