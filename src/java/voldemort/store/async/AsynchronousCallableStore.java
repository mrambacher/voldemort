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
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * An AsynchronousCallableStore contains a CallableStore and can submit
 * asynchronous requests.
 * 
 * An AsynchronousCallable stores contains a CallableStore and acts like an
 * asynchronous one. Extensions of this class must implement the submit method,
 * which runs the
 * 
 * @param <K> The type of keys for this store.
 * @param <V> The type of values for this store.
 */
abstract public class AsynchronousCallableStore<K, V, T> implements AsynchronousStore<K, V, T> {

    private final CallableStore<K, V, T> inner;

    protected AsynchronousCallableStore(CallableStore<K, V, T> inner) {
        this.inner = inner;
    }

    /**
     * Submits the callable operation to the store.
     * 
     * @param operation The operation to invoke
     * @param callable The task to invoke
     * @return The store future representing this task.
     */
    abstract protected <R> StoreFuture<R> submit(AsynchronousStore.Operations operation,
                                                 Callable<R> callable);

    public StoreFuture<List<Versioned<V>>> submitGet(final K key, final T transform)
            throws VoldemortException {
        Callable<List<Versioned<V>>> callable = inner.callGet(key, transform);
        return submit(AsynchronousStore.Operations.GET, callable);
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
        Callable<Map<K, List<Versioned<V>>>> callable = inner.callGetAll(keys, transforms);
        return submit(AsynchronousStore.Operations.GETALL, callable);
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public StoreFuture<Version> submitPut(K key, Versioned<V> value, final T transform)
            throws VoldemortException {
        Callable<Version> callable = inner.callPut(key, value, transform);
        return submit(AsynchronousStore.Operations.PUT, callable);
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public StoreFuture<Boolean> submitDelete(K key, Version version) throws VoldemortException {
        Callable<Boolean> callable = inner.callDelete(key, version);
        return submit(AsynchronousStore.Operations.DELETE, callable);
    }

    public StoreFuture<List<Version>> submitGetVersions(K key) throws VoldemortException {
        Callable<List<Version>> callable = inner.callGetVersions(key);
        return submit(AsynchronousStore.Operations.GETVERSIONS, callable);
    }

    public String getName() {
        return inner.getName();
    }

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
        switch(capability) {
            case ASYNCHRONOUS:
                return this;
            default:
                return inner.getCapability(capability);
        }
    }
}
