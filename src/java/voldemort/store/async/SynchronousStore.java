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
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Converts an asynchronous store into a Synchronous (@link Store) one. A
 * synchronous store submits store futures and waits for them to complete.
 */
public class SynchronousStore<K, V, T> extends DelegatingAsynchronousStore<K, V, T> implements
        Store<K, V, T> {

    private final long timeout;
    private final TimeUnit units;

    /**
     * Creates a synchronous store from an asynchronous one.
     * 
     * @param async The wrapped asynchronous store
     */
    public SynchronousStore(AsynchronousStore<K, V, T> async) {
        this(async, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a synchronous store from an asynchronous one.
     * 
     * @param async The wrapped asynchronous store
     * @param timeout How long to wait for the asynchronous task to complete (0
     *        = forever)
     * @param units What units the timeout is expressed in.
     */
    public SynchronousStore(AsynchronousStore<K, V, T> async, long timeout, TimeUnit units) {
        super(async);
        this.timeout = timeout;
        this.units = units;
    }

    /**
     * Waits for store future to complete and returns the result
     * 
     * @param future The future to complete
     * @return The result of the future
     */
    @SuppressWarnings("unused")
    protected <R> R awaitCompletion(Operation operation, StoreFuture<R> future)
            throws VoldemortException {
        if(timeout > 0) {
            return future.get(timeout, units);
        } else {
            return future.get();
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
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        StoreFuture<List<Versioned<V>>> future = submitGet(key, transform);
        return awaitCompletion(Operation.GET, future);
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
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreFuture<Map<K, List<Versioned<V>>>> future = submitGetAll(keys, transforms);
        return awaitCompletion(Operation.GET_ALL, future);
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public Version put(K key, Versioned<V> value, T transform) throws VoldemortException {
        StoreFuture<Version> future = submitPut(key, value, transform);
        return awaitCompletion(Operation.PUT, future);
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public boolean delete(K key, Version version) throws VoldemortException {
        StoreFuture<Boolean> future = submitDelete(key, version);
        return awaitCompletion(Operation.DELETE, future);
    }

    public List<Version> getVersions(K key) throws VoldemortException {
        StoreFuture<List<Version>> future = submitGetVersions(key);
        return awaitCompletion(Operation.GET_VERSIONS, future);
    }
}
