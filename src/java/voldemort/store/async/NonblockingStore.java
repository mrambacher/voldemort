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

/**
 * Nonblocking stores allow a Callable store to treated as an asynchronous
 * store.
 */
public class NonblockingStore<K, V, T> extends AsynchronousCallableStore<K, V, T> {

    public NonblockingStore(CallableStore<K, V, T> store) {
        super(store);
    }

    /**
     * Creates a callable (inline) future for this task.
     */
    @Override
    protected <R> StoreFuture<R> submit(AsynchronousStore.Operations operation, Callable<R> task) {
        return new CallableFuture<R>(operation.name(), task);
    }
}
