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

import voldemort.serialization.VoldemortOpCode;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * A distributed store that runs requests in parallel and waits for the
 * preferred/required nodes to complete.
 */
public class DistributedParallelStore<N, K, V, T> extends AbstractDistributedStore<N, K, V, T> {

    public static <N, K, V, T> DistributedStore<N, K, V, T> create(Map<N, AsynchronousStore<K, V, T>> stores,
                                                                   StoreDefinition storeDef,
                                                                   boolean unique) {
        return new DistributedParallelStore<N, K, V, T>(stores, storeDef, unique);
    }

    public DistributedParallelStore(Map<N, AsynchronousStore<K, V, T>> stores,
                                    StoreDefinition storeDef,
                                    boolean unique) {
        super(stores, storeDef, unique);
    }

    protected <R> DistributedFutureTask<N, R> submit(VoldemortOpCode operation,
                                                     Map<N, StoreFuture<R>> futures,
                                                     ResultsBuilder<N, R> builder,
                                                     int available,
                                                     int preferred,
                                                     int required) {
        return new DistributedFutureTask<N, R>(operation.getMethodName(),
                                               futures,
                                               builder,
                                               available,
                                               preferred,
                                               required);
    }

    protected <R> DistributedFutureTask<N, R> submit(VoldemortOpCode operation,
                                                     Map<N, StoreFuture<R>> futures,
                                                     ResultsBuilder<N, R> builder,
                                                     int preferred,
                                                     int required) {
        return submit(operation, futures, builder, futures.size(), preferred, required);
    }

    @Override
    protected DistributedFuture<N, List<Versioned<V>>> distributeGet(Collection<N> nodes,
                                                                     final K key,
                                                                     final T transform,
                                                                     int preferred,
                                                                     int required) {
        Map<N, StoreFuture<List<Versioned<V>>>> futures = Maps.newHashMap();
        for(N node: nodes) {
            futures.put(node, submitGet(node, key, transform));
        }
        return submit(VoldemortOpCode.GET, futures, getBuilder, preferred, required);
    }

    @Override
    protected DistributedFuture<N, Map<K, List<Versioned<V>>>> distributeGetAll(final Map<N, List<K>> nodesToKeys,
                                                                                final Map<K, T> transforms,
                                                                                int preferred,
                                                                                int required) {
        Map<N, StoreFuture<Map<K, List<Versioned<V>>>>> futures = Maps.newHashMap();
        for(Map.Entry<N, List<K>> entry: nodesToKeys.entrySet()) {
            futures.put(entry.getKey(), submitGetAll(entry.getKey(), entry.getValue(), transforms));
        }
        ResultsBuilder<N, Map<K, List<Versioned<V>>>> getAllBuilder = DistributedStoreFactory.GetAllBuilder(nodesToKeys,
                                                                                                            required,
                                                                                                            makeUnique);
        return submit(VoldemortOpCode.GET_ALL, futures, getAllBuilder, nodesToKeys.size(), 1);
    }

    @Override
    protected DistributedFuture<N, Version> distributePut(Collection<N> nodes,
                                                          K key,
                                                          Versioned<V> value,
                                                          T transform,
                                                          int preferred,
                                                          int required) {
        Map<N, StoreFuture<Version>> futures = Maps.newHashMap();
        for(N node: nodes) {
            futures.put(node, submitPut(node, key, value, transform));
        }
        return submit(VoldemortOpCode.PUT, futures, this.putBuilder, preferred, required);
    }

    @Override
    protected DistributedFuture<N, Boolean> distributeDelete(Collection<N> nodes,
                                                             K key,
                                                             Version version,
                                                             int preferred,
                                                             int required) {
        Map<N, StoreFuture<Boolean>> futures = Maps.newHashMap();
        for(N node: nodes) {
            futures.put(node, submitDelete(node, key, version));
        }
        return submit(VoldemortOpCode.DELETE, futures, this.deleteBuilder, preferred, required);
    }

    @Override
    protected DistributedFuture<N, List<Version>> distributeGetVersions(Collection<N> nodes,
                                                                        K key,
                                                                        int preferred,
                                                                        int required) {
        Map<N, StoreFuture<List<Version>>> futures = Maps.newHashMap();
        for(N node: nodes) {
            futures.put(node, submitGetVersions(node, key));
        }
        return submit(VoldemortOpCode.GET_VERSION,
                      futures,
                      this.versionsBuilder,
                      preferred,
                      required);
    }
}
