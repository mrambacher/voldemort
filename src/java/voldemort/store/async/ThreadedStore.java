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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import voldemort.serialization.VoldemortOpCode;
import voldemort.store.Store;

/**
 * An asynchronous store that runs store tasks in a thread pool.
 */
public class ThreadedStore<K, V, T> extends AsynchronousCallableStore<K, V, T> {

    protected final ExecutorService executor;

    /**
     * Returns the ThreadedStore wrapping the callable store.
     * 
     * @param inner The store to wrap
     * @param executor The thread pool to use
     * @return The threaded store
     */
    public static <K, V, T> ThreadedStore<K, V, T> create(Store<K, V, T> store,
                                                          ExecutorService executor) {
        return new ThreadedStore<K, V, T>(store, executor);
    }

    /**
     * Returns the ThreadedStore wrapping the callable store.
     * 
     * @param inner The store to wrap
     * @param executor The thread pool to use
     * @return The threaded store
     */
    public ThreadedStore(Store<K, V, T> inner, ExecutorService executor) {
        super(AsyncUtils.asCallable(inner));
        this.executor = executor;
    }

    /**
     * Returns the ThreadedStore wrapping the callable store.
     * 
     * @param inner The store to wrap
     * @param executor The thread pool to use
     * @return The threaded store
     */
    public ThreadedStore(CallableStore<K, V, T> inner, ExecutorService executor) {
        super(inner);
        this.executor = executor;
    }

    /**
     * Submits the callable operation to the thread pool
     * 
     * @param operation The operation to invoke
     * @param callable The task to invoke
     * @return The store future representing this asynchronous task.
     */
    @Override
    protected <R> StoreFuture<R> submit(VoldemortOpCode operation, Callable<R> callable) {
        RunnableFutureTask<R> task = new RunnableFutureTask<R>(operation.getOperationName(), callable);
        executor.execute(task);
        return task;
    }
}
