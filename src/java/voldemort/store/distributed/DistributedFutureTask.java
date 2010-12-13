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
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;

import com.google.common.collect.Maps;

/**
 * Implementation class for a basic distributed future. This future task takes
 * in a collection of futures and a builder. This future waits for "preferred"
 * futures to completes but returns success if "required" futures completed
 * successfully.
 * */
public class DistributedFutureTask<N, V> extends AbstractDistributedFuture<N, V> {

    /** The set of futures associated with this one. */
    protected Map<N, StoreFuture<V>> futures;
    private StoreFutureListener<V> listener;

    public DistributedFutureTask(String operation,
                                 Map<N, StoreFuture<V>> futures,
                                 ResultsBuilder<N, V> builder,
                                 int preferred,
                                 int required) {
        this(operation, futures, builder, futures.size(), preferred, required);
    }

    /**
     * Creates a new distributed future task
     * 
     * @param operation The name of the operation for this future (e.g. "GET")
     * @param futures The set of futures upon which to wait for completion.
     * @param builder Class used to build the results returned by this future.
     * @param preferred How many futures to wait for completion of.
     * @param required How many futures must complete successfully for this
     *        future to be successful.
     */
    public DistributedFutureTask(String operation,
                                 Map<N, StoreFuture<V>> futures,
                                 ResultsBuilder<N, V> builder,
                                 int available,
                                 int preferred,
                                 int required) {
        super(operation, builder, available, preferred, required);
        this.futures = Maps.newHashMap(futures);
        this.listener = new StoreFutureListener<V>() {

            @SuppressWarnings("unchecked")
            public void futureCompleted(Object arg, V result, long duration) {
                N node = (N) arg;
                nodeCompleted(node, result, duration);

            }

            @SuppressWarnings("unchecked")
            public void futureFailed(Object arg, VoldemortException exception, long duration) {
                N node = (N) arg;
                nodeFailed(node, exception, duration);

            }
        };
        this.submitted = System.nanoTime();
        for(Map.Entry<N, StoreFuture<V>> entry: this.futures.entrySet()) {
            N node = entry.getKey();
            StoreFuture<V> future = entry.getValue();
            future.register(listener, node);
        }
    }

    protected void addFuture(N node, StoreFuture<V> future) {
        if(!this.isDone() && !isCancelled()) {
            this.submitted = System.nanoTime();
            futures.put(node, future);
            future.register(listener, node);
        }
    }

    /**
     * Returns the set of nodes that are associated with this future.
     */
    public Collection<N> getNodes() {
        return futures.keySet();
    }

    public StoreFuture<V> getFuture(N node) {
        return futures.get(node);
    }
}
