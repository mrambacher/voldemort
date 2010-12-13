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

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Class to convert a Store into a Callable store.
 */
public class WrappedCallableStore<K, V, T> implements CallableStore<K, V, T> {

    protected Store<K, V, T> inner;

    public WrappedCallableStore(Store<K, V, T> inner) {
        this.inner = inner;
    }

    /**
     * Returns the callable. Extenders of this class may override this method to
     * add their own functionality to the call chain.
     * 
     * @param inner The callable method to run.
     * @return The input callable
     */
    protected <R> Callable<R> wrap(Callable<R> inner) {
        return inner;
    }

    /**
     * Returns the result of the callable task. This method translates the
     * exceptions from the task into their Voldemort equivalents.
     */
    public <R> R call(Callable<R> task) {
        try {
            return task.call();
        } catch(VoldemortException e) {
            throw e;
        } catch(Exception e) {
            throw new VoldemortException(e);
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
    public Callable<List<Versioned<V>>> callGet(final K key, final T transform)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Callable<List<Versioned<V>>> task = new Callable<List<Versioned<V>>>() {

            public List<Versioned<V>> call() {
                return inner.get(key, transform);
            }
        };
        return wrap(task);
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
    public Callable<Map<K, List<Versioned<V>>>> callGetAll(final Iterable<K> keys,
                                                           final Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Callable<Map<K, List<Versioned<V>>>> task = new Callable<Map<K, List<Versioned<V>>>>() {

            public Map<K, List<Versioned<V>>> call() {
                return inner.getAll(keys, transforms);
            }
        };
        return wrap(task);
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public Callable<Version> callPut(final K key, final Versioned<V> value, final T transform)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Callable<Version> task = new Callable<Version>() {

            public Version call() {
                return inner.put(key, value, transform);
            }
        };
        return wrap(task);
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
        StoreUtils.assertValidKey(key);
        Callable<Boolean> task = new Callable<Boolean>() {

            public Boolean call() {
                return inner.delete(key, version);
            }
        };
        return wrap(task);
    }

    public Callable<List<Version>> callGetVersions(final K key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Callable<List<Version>> task = new Callable<List<Version>>() {

            public List<Version> call() {
                return inner.getVersions(key);
            }
        };
        return wrap(task);
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
