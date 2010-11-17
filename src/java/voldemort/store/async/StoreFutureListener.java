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

import voldemort.VoldemortException;

/**
 * Interface for registering for future completion. When a StoreFuture
 * completes, all registered listeners are invoked in the order in which they
 * were registered. When a future completes, either the completed or failed
 * listener will be invoked (not both). Additionally, each listener will be
 * invoked exactly once for each future.
 */
public interface StoreFutureListener<V> {

    /**
     * Method invoked when a future completes successfully.
     * 
     * @param key The key provided when this listener was registered
     * @param result The result of the future
     * @param durationNs How long the future took to complete (in nanoseconds).
     */
    public void futureCompleted(Object key, V result, long durationNs);

    /**
     * Method invoked when a future throws an exception.
     * 
     * @param key The key provided when this listener was registered
     * @param ex The exception thrown by the future
     * @param durationNs How long the future took to complete (in nanoseconds).
     */
    public void futureFailed(Object key, VoldemortException ex, long durationNs);
}
