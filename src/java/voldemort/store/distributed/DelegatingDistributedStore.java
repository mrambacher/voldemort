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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.async.AsynchronousStore;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Wrapper class to allow extenders to add functionality to a distributed store.
 * This class is-a/has-a DistributedStore.
 */
public class DelegatingDistributedStore<N, K, V, T> implements DistributedStore<N, K, V, T> {

    protected final Logger logger = Logger.getLogger(getClass());

    private final DistributedStore<N, K, V, T> inner;

    /**
     * Create a new delegating distributed store
     * 
     * @param inner The wrapped distributed store.
     */
    public DelegatingDistributedStore(DistributedStore<N, K, V, T> inner) {
        this.inner = inner;
    }

    /**
     * Returns the set of nodes for the key from the inner store.
     * 
     * @param key The key to retrieve the nodes for
     * @return The collection of nodes for the given key.
     */
    public Collection<N> getNodesForKey(final K key) {
        return inner.getNodesForKey(key);
    }

    /**
     * Enhances the future returned for the store. This method is meant as a
     * place where derived classes can add their own functionality without
     * overriding every method. For example, some implementations might register
     * listeners while others might return a wrapped future.
     * 
     * @param future The future job for the task
     * @return The enriched future task.
     */
    protected <R> DistributedFuture<N, R> buildFuture(DistributedFuture<N, R> future) {
        return future;
    }

    /**
     * Returns the store for the given node. Delegated to the inner store.
     * 
     * @param node The node being queried
     * @return The asynchronous store for the input node.
     */
    public AsynchronousStore<K, V, T> getNodeStore(N node) {
        return inner.getNodeStore(node);
    }

    /**
     * Returns the node stores associated with this store. Delegated to the
     * inner store.
     * 
     * @return The asynchronous stores for this distributor.
     */
    public Map<N, AsynchronousStore<K, V, T>> getNodeStores() {
        return inner.getNodeStores();
    }

    /**
     * Get the value associated with the given key. The request is delegated to
     * the inner store but the returned value is wrapped by buildFuture.
     * 
     * @param nodes The collection of nodes to query for the key
     * @param key The key to check for
     * @param preferred How many nodes to wait for a response from
     * @param required How many nodes must respond for the operation to be
     *        successful.
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    public DistributedFuture<N, List<Versioned<V>>> submitGet(final K key,
                                                              final T transform,
                                                              Collection<N> nodes,
                                                              int preferred,
                                                              int required)
            throws VoldemortException {
        return buildFuture(inner.submitGet(key, transform, nodes, preferred, required));
    }

    /**
     * Get the values associated with the given keys and returns them in a Map
     * of keys to a list of versioned values. Note that the returned map will
     * only contain entries for the keys which have a value associated with
     * them.
     * 
     * The request is delegated to the inner store but the returned value is
     * wrapped by buildFuture.
     * 
     * @param nodesToKeys The collection of nodes, and which keys to expect for
     *        each of them.
     * @param preferred How many nodes to wait for a response from for each key.
     * @param required How many nodes must respond for the operation to be
     *        successful.
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    public DistributedFuture<N, Map<K, List<Versioned<V>>>> submitGetAll(final Map<N, List<K>> nodesToKeys,
                                                                         final Map<K, T> transforms,
                                                                         int preferred,
                                                                         int required)
            throws VoldemortException {
        return buildFuture(inner.submitGetAll(nodesToKeys, transforms, preferred, required));

    }

    /**
     * Associate the value with the key and version in this store The request is
     * delegated to the inner store but the returned value is wrapped by
     * buildFuture.
     * 
     * @param nodes The collection of nodes for this operation.
     * @param key The key to use
     * @param value The value to store and its version.
     * @param preferred How many nodes to wait for a response from
     * @param required How many nodes must respond for the operation to be
     *        successful.
     */
    public DistributedFuture<N, Version> submitPut(K key,
                                                   Versioned<V> value,
                                                   final T transform,
                                                   Collection<N> nodes,
                                                   int preferred,
                                                   int required) throws VoldemortException {
        return buildFuture(inner.submitPut(key, value, transform, nodes, preferred, required));
    }

    /**
     * Delete all entries prior to the given version The request is delegated to
     * the inner store but the returned value is wrapped by buildFuture.
     * 
     * @param nodes The collection of nodes for this operation.
     * @param key The key to delete
     * @param version The current value of the key
     * @param value The value to store and its version.
     * @param preferred How many nodes to wait for a response from
     * @param required How many nodes must respond for the operation to be
     *        successful.
     * @return True if anything was deleted
     */
    public DistributedFuture<N, Boolean> submitDelete(K key,
                                                      Version version,
                                                      Collection<N> nodes,
                                                      int preferred,
                                                      int required) throws VoldemortException {
        return buildFuture(inner.submitDelete(key, version, nodes, preferred, required));
    }

    /**
     * Get the versions associated with the given key The request is delegated
     * to the inner store but the returned value is wrapped by buildFuture.
     * 
     * @param nodes The collection of nodes to query for the key
     * @param key The key to check for
     * @param preferred How many nodes to wait for a response from
     * @param required How many nodes must respond for the operation to be
     *        successful.
     * @return The versions associated with the key or an empty list if no
     *         values are found.
     * @throws VoldemortException
     */
    public DistributedFuture<N, List<Version>> submitGetVersions(K key,
                                                                 Collection<N> nodes,
                                                                 int preferred,
                                                                 int required)
            throws VoldemortException {
        return buildFuture(inner.submitGetVersions(key, nodes, preferred, required));
    }

    /**
     * @return The name of the store.
     */
    public String getName() {
        return inner.getName();
    }

    public void close() throws VoldemortException {
        inner.close();
    }

    public Object getCapability(StoreCapabilityType capability) throws VoldemortException {
        switch(capability) {
            case ASYNCHRONOUS:
                return this;
            default:
                return inner.getCapability(capability);
        }
    }
}
