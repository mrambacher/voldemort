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
    public static <K, V, T> Store<K, V, T> asSync(AsynchronousStore<K, V, T> async) {
        return new SynchronousStore<K, V, T>(async);
    }

    /**
     * Converts a synchronous store into its asynchronous equivalent.
     * 
     * @param callable The store to be converted
     * @return The asynchronous equivalent
     */
    public static <K, V, T> AsynchronousStore<K, V, T> asAsync(CallableStore<K, V, T> callable) {
        return new NonblockingStore<K, V, T>(callable);
    }

    /**
     * Converts an asynchronous store into its synchronous (blocking)
     * equivalent.
     * 
     * @param sync The store to be converted
     * @return The synchronous equivalent
     */
    @SuppressWarnings("unchecked")
    public static <K, V, T> AsynchronousStore<K, V, T> asAsync(Store<K, V, T> sync) {
        try {
            return (AsynchronousStore<K, V, T>) sync.getCapability(StoreCapabilityType.ASYNCHRONOUS);
        } catch(NoSuchCapabilityException e) {
            return new NonblockingStore<K, V, T>(asCallable(sync));
        }
    }

    /**
     * Converts a synchronous store from its callable equivalent.
     * 
     * @param callable The callable store to be converted
     * @return The synchronous equivalent
     */
    public static <K, V, T> Store<K, V, T> asStore(CallableStore<K, V, T> callable) {
        return asStore(asAsync(callable));
    }

    /**
     * Converts a synchronous store from its asynchronous equivalent.
     * 
     * @param async The asynchronous store to be converted
     * @return The synchronous equivalent
     */
    public static <K, V, T> Store<K, V, T> asStore(AsynchronousStore<K, V, T> async) {
        return new SynchronousStore<K, V, T>(async);
    }

    /**
     * Converts a synchronous store from its asynchronous equivalent.
     * 
     * @param async The asynchronous store to be converted
     * @return The synchronous equivalent
     */
    public static <N, K, V, T> Map<N, Store<K, V, T>> asStores(Map<N, AsynchronousStore<K, V, T>> asyncs) {
        Map<N, Store<K, V, T>> stores = Maps.newHashMap();
        for(Map.Entry<N, AsynchronousStore<K, V, T>> entry: asyncs.entrySet()) {
            stores.put(entry.getKey(), AsyncUtils.asStore(entry.getValue()));
        }
        return stores;
    }

    /**
     * Converts a synchronous store into its callable equivalent.
     * 
     * @param store The store to be converted
     * @return The callable equivalent
     */
    public static <K, V, T> CallableStore<K, V, T> asCallable(Store<K, V, T> store) {
        return new WrappedCallableStore<K, V, T>(store);
    }
}
