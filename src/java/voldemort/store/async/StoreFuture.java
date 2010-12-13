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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;

/**
 * The basic interface used for returning asynchronous store results.
 */
public interface StoreFuture<V> extends Future<V> {

    /**
     * Returns how long the future took to complete or has been running
     * 
     * @param units The units to return the the duration in.
     * @return How long the future took to complete or has been running
     */
    public long getDuration(TimeUnit units);

    public long getRemaining(long timeout, TimeUnit units);

    /**
     * Registers a completion listener with the future. The listener will be
     * invoked when the future completes. If the future has already completed,
     * the callback will be invoked immediately.
     * 
     * @param listener
     */
    public void register(StoreFutureListener<V> listener, Object arg);

    /**
     * Returns the name of the operation (@link AsynchronousStore.Operations)
     * running in this future.
     * 
     * @return The name of the operation.
     */
    public String getOperation();

    /**
     * Returns the result of the future. If the future is still running, this
     * method blocks until the future completes.
     * 
     * @return The result of the future.
     */
    public V get() throws VoldemortException;

    /**
     * Returns the result of the future. If the future is still running, this
     * method waits until the future completes or until the timeout expires
     * 
     * @param timeout How long to wait for the future to complete
     * @param units The units of the timeout.
     * @return The result of the future.
     */
    public V get(long timeout, TimeUnit units) throws VoldemortException;
}
