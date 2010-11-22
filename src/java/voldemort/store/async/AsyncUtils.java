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

import java.util.Map;

import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;

import com.google.common.collect.Maps;

/**
 * Utility class for converting to and from asynchronous stores.
 */
public class AsyncUtils {

    /**
     * Converts an asynchronous store into its synchronous (blocking)
     * equivalent.
     * 
     * @param async The store to be converted
     * @return The synchronous equivalent
     */
    public static <K, V> Store<K, V> asSync(AsynchronousStore<K, V> async) {
        return new SynchronousStore<K, V>(async);
    }

    /**
     * Converts a synchronous store into its asynchronous equivalent.
     * 
     * @param callable The store to be converted
     * @return The asynchronous equivalent
     */
    public static <K, V> AsynchronousStore<K, V> asAsync(CallableStore<K, V> callable) {
        return new NonblockingStore<K, V>(callable);
    }

    /**
     * Converts an asynchronous store into its synchronous (blocking)
     * equivalent.
     * 
     * @param sync The store to be converted
     * @return The synchronous equivalent
     */
    @SuppressWarnings("unchecked")
    public static <K, V> AsynchronousStore<K, V> asAsync(Store<K, V> sync) {
        try {
            return (AsynchronousStore<K, V>) sync.getCapability(StoreCapabilityType.ASYNCHRONOUS);
        } catch(NoSuchCapabilityException e) {
            return new NonblockingStore<K, V>(asCallable(sync));
        }
    }

    /**
     * Converts a synchronous store from its callable equivalent.
     * 
     * @param callable The callable store to be converted
     * @return The synchronous equivalent
     */
    public static <K, V> Store<K, V> asStore(CallableStore<K, V> callable) {
        return asStore(asAsync(callable));
    }

    /**
     * Converts a synchronous store from its asynchronous equivalent.
     * 
     * @param async The asynchronous store to be converted
     * @return The synchronous equivalent
     */
    public static <K, V> Store<K, V> asStore(AsynchronousStore<K, V> async) {
        return new SynchronousStore<K, V>(async);
    }

    /**
     * Converts a synchronous store into its callable equivalent.
     * 
     * @param store The store to be converted
     * @return The callable equivalent
     */
    public static <K, V> CallableStore<K, V> asCallable(Store<K, V> store) {
        return new WrappedCallableStore<K, V>(store);
    }

    /**
     * Converts the map of asynchronous stores to a map of synchronous ones.
     * 
     * @param asyncs The map of asynchronous stores
     * @return The synchronous equivalent
     */
    public static <N, K, V> Map<N, Store<K, V>> asStores(Map<N, AsynchronousStore<K, V>> asyncs) {
        Map<N, Store<K, V>> stores = Maps.newHashMap();
        for(Map.Entry<N, AsynchronousStore<K, V>> entry: asyncs.entrySet()) {
            stores.put(entry.getKey(), asStore(entry.getValue()));
        }
        return stores;
    }
}
