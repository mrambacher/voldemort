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
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.store.async.StoreFuture;

/**
 * The basic interface used for returning distributed store results. A
 * distributed future contains a collection of futures and returns based on the
 * results of the wrapped futures. A distribute future represents a collected
 * result and not an external task.
 */
public interface DistributedFuture<N, V> extends StoreFuture<V> {

    /**
     * Returns the number of futures that have successfully completed (without
     * error)
     * 
     * @return The number of successful futures
     */
    public int getCompleted();

    /**
     * Returns the result of the distributed future. This method blocks until
     * all of the wrapped futures complete or the timeout expires
     * 
     * @param timeout How long to wait for the wrapped futures to complete
     * @param units The units of the timeout.
     * @return The result of the future.
     */
    Map<N, V> complete(long timeout, TimeUnit units) throws VoldemortException;

    /**
     * Returns the result of the distributed future. This method blocks until
     * all of the wrapped futures complete.
     * 
     * @return The result of the future.
     */
    Map<N, V> complete() throws VoldemortException;

    public boolean isDone(N node);

    /**
     * Returns true if the future has completed successfully, and false
     * otherwise
     */
    public boolean isSuccessful();

    /**
     * Returns the collection of successful results for this future.
     * 
     * @return
     */
    public Map<N, V> getResults();

    /**
     * Returns how long the specific wrapped future took to complete or has been
     * running
     * 
     * @param node The future to query.
     * @param units The units to return the the duration in.
     * @return How long the future took to complete or has been running
     */
    public long getDuration(N node, TimeUnit units);

    /**
     * Returns the set of nodes for the wrapped futures.
     */
    public Collection<N> getNodes();
}
