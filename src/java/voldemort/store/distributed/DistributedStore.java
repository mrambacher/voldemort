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

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.async.AsynchronousStore;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The basic interface used for distributed store operations. The methods here
 * are the asynchronous equivalents of those in @link Store}. The Distributed
 * Store methods send the request to a collection of nodes and wait for the
 * "preferred" number of responses. If at least "required" nodes responded
 * successfully, the operation will return successfully. If fewer nodes respond
 * successfully, an exception will be thrown.
 */
public interface DistributedStore<N, K, V, T> {

    /**
     * Returns the asynchronous store associated with the input node.
     * 
     * @param node The node to return the store for.
     * @return The asynchronous store for the node.
     */
    public AsynchronousStore<K, V, T> getNodeStore(N node);

    /**
     * Returns the asynchronous stores and their associated node.
     * 
     * @return The map of node to asynchronous store.
     */
    public Map<N, AsynchronousStore<K, V, T>> getNodeStores();

    /**
     * Returns the set of nodes that a given key should be stored on.
     * 
     * @param key The key being queried
     * @return The collection of nodes for that key.
     */
    public Collection<N> getNodesForKey(final K key);

    /**
     * Get the value associated with the given key
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
            throws VoldemortException;

    /**
     * Get the values associated with the given keys and returns them in a Map
     * of keys to a list of versioned values. Note that the returned map will
     * only contain entries for the keys which have a value associated with
     * them.
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
                                                                         Map<K, T> transforms,
                                                                         int preferred,
                                                                         int required)
            throws VoldemortException;

    /**
     * Associate the value with the key and version in this store
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
                                                   int required) throws VoldemortException;

    /**
     * Delete all entries prior to the given version
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
                                                      int required) throws VoldemortException;

    /**
     * Get the versions associated with the given key
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
            throws VoldemortException;

    /**
     * @return The name of the store.
     */
    public String getName();

    /**
     * Close the store.
     * 
     * @throws VoldemortException If closing fails.
     */
    public void close() throws VoldemortException;

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
    public Object getCapability(StoreCapabilityType capability);
}
