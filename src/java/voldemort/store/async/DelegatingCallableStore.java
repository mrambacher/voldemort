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
import java.util.concurrent.Callable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Wrapper class to allow extenders to add functionality to a callable store.
 * This class is-a/has-a callable store.
 */
public class DelegatingCallableStore<K, V> implements CallableStore<K, V> {

    protected final Logger logger = LogManager.getLogger(getClass());

    private CallableStore<K, V> inner;

    protected DelegatingCallableStore(CallableStore<K, V> inner) {
        this.inner = inner;
    }

    /**
     * Extension point to add functionality to all callable methods of the
     * store.
     * 
     * @param inner The callable method
     * @return The callable method.
     */
    protected <R> Callable<R> wrap(Callable<R> inner) {
        return inner;
    }

    /**
     * Get the value associated with the given key
     * 
     * @param key The key to check for
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    public Callable<List<Versioned<V>>> callGet(final K key) throws VoldemortException {
        return wrap(inner.callGet(key));
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
    public Callable<Map<K, List<Versioned<V>>>> callGetAll(final Iterable<K> keys)
            throws VoldemortException {
        return wrap(inner.callGetAll(keys));
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public Callable<Version> callPut(final K key, final Versioned<V> value)
            throws VoldemortException {
        return wrap(inner.callPut(key, value));
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public Callable<Boolean> callDelete(final K key, final Version version)
            throws VoldemortException {
        return wrap(inner.callDelete(key, version));
    }

    public Callable<List<Version>> callGetVersions(final K key) throws VoldemortException {
        return wrap(inner.callGetVersions(key));
    }

    /**
     * @return The name of the store.
     */
    public String getName() {
        return inner.getName();
    }

    /**
     * Close the store.
     * 
     * @throws VoldemortException If closing fails.
     */
    public void close() throws VoldemortException {
        inner.close();
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
        return inner.getCapability(capability);
    }

}
