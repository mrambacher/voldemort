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

package voldemort.store.async;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The basic interface used for asynchronous storage operations. The methods
 * here are the asynchronous equivalents of those in @link Store}.
 */
public interface AsynchronousStore<K, V, T> {

    public enum Operations {
        GET("GET"),
        GETALL("GETALL"),
        PUT("PUT"),
        DELETE("DELETE"),
        GETVERSIONS("GETVERSIONS");

        private String name;

        private Operations(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }

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
            throws VoldemortException;

    /**
     * Get the versions associated with the given key
     * 
     * @param key The key to check for
     * @return The versions associated with the key or an empty list if no
     *         versions are found.
     * @throws VoldemortException
     */
    public StoreFuture<List<Version>> submitGetVersions(K key);

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
                                                                final Map<K, T> transform)
            throws VoldemortException;

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public StoreFuture<Version> submitPut(K key, Versioned<V> value, final T transform)
            throws VoldemortException;

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public StoreFuture<Boolean> submitDelete(K key, Version version) throws VoldemortException;

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
