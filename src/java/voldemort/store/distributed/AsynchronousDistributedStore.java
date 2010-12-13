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
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * Allows a distributed store to be treated as an asynchronous one.
 */
public class AsynchronousDistributedStore<N, K, V, T> extends
        DelegatingDistributedStore<N, K, V, T> implements AsynchronousStore<K, V, T> {

    private final StoreDefinition storeDef;

    /**
     * Create a new distributed store
     * 
     * @param storeDef The store definition for this store.
     * @param inner The underlying distributed store.
     */
    public AsynchronousDistributedStore(StoreDefinition storeDef, DistributedStore<N, K, V, T> inner) {
        super(inner);
        this.storeDef = storeDef;
    }

    /**
     * Returns the number of reads preferred for a distributed operation.
     */
    protected int getPreferredReads() {
        return storeDef.getPreferredReads();
    }

    /**
     * Returns the number of reads required for a distributed operation.
     */
    protected int getRequiredReads() {
        return storeDef.getRequiredReads();
    }

    /**
     * Returns the number of writes preferred for a distributed operation.
     */
    protected int getPreferredWrites() {
        return storeDef.getPreferredWrites();
    }

    /**
     * Returns the number of writes required for a distributed operation.
     */
    protected int getRequiredWrites() {
        return storeDef.getRequiredWrites();
    }

    /**
     * Checks to see if enough nodes are available for the read to complete
     * successfully.
     * 
     * @param nodes The nodes for the operation
     * @param required The number of nodes required for the operation
     * @throws InsufficientOperationalNodesException If too few nodes are in the
     *         list.
     */
    protected void checkRequiredReads(final Collection<N> nodes, int required)
            throws InsufficientOperationalNodesException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
    }

    /**
     * Checks to see if enough nodes are available for the write to complete
     * successfully.
     * 
     * @param nodes The nodes for the operation
     * @param required The number of nodes required for the operation
     * @throws InsufficientOperationalNodesException If too few nodes are in the
     *         list.
     */
    protected void checkRequiredWrites(final Collection<N> nodes, int required)
            throws InsufficientOperationalNodesException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
    }

    /**
     * Get the value associated with the given key
     * 
     * @param key The key to check for
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    public StoreFuture<List<Versioned<V>>> submitGet(final K key, final T transform)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Collection<N> nodes = getNodesForKey(key);
        checkRequiredReads(nodes, getRequiredReads());
        return this.submitGet(key, transform, nodes, getPreferredReads(), getRequiredReads());
    }

    protected Map<N, List<K>> getNodesForKeys(Iterable<K> keys) throws VoldemortException {
        Map<N, List<K>> nodeToKeys = Maps.newHashMap();
        for(K key: keys) {
            Collection<N> nodes = getNodesForKey(key);
            checkRequiredReads(nodes, getPreferredReads());
            for(N node: nodes) {
                List<K> keysForNode = nodeToKeys.get(node);
                if(keysForNode == null) {
                    keysForNode = new ArrayList<K>();
                    nodeToKeys.put(node, keysForNode);
                }
                keysForNode.add(key);
            }
        }
        return nodeToKeys;
    }

    /**
     * Get the values associated with the given keys and returns them in a Map
     * of keys to a list of versioned values. Note that the returned map will
     * only contain entries for the keys which have a value associated with
     * them.
     * 
     * @param keys The keys to check for.
     * @return A Map of keys to a list of versioned values.
     * @throws VoldemortException
     */
    public StoreFuture<Map<K, List<Versioned<V>>>> submitGetAll(final Iterable<K> keys,
                                                                final Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<N, List<K>> nodesToKeys = getNodesForKeys(keys);
        return this.submitGetAll(nodesToKeys, transforms, getPreferredReads(), getRequiredReads());
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public StoreFuture<Version> submitPut(K key, Versioned<V> value, final T transform)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Collection<N> nodes = getNodesForKey(key);
        checkRequiredWrites(nodes, getRequiredWrites());
        return this.submitPut(key,
                              value,
                              transform,
                              nodes,
                              getPreferredWrites(),
                              getRequiredWrites());
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public StoreFuture<Boolean> submitDelete(K key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Collection<N> nodes = getNodesForKey(key);
        checkRequiredWrites(nodes, getRequiredWrites());
        return this.submitDelete(key, version, nodes, getPreferredWrites(), getRequiredWrites());
    }

    public StoreFuture<List<Version>> submitGetVersions(K key) {
        StoreUtils.assertValidKey(key);
        Collection<N> nodes = getNodesForKey(key);
        checkRequiredReads(nodes, getRequiredReads());
        return this.submitGetVersions(key, nodes, getPreferredReads(), getRequiredReads());
    }
}
