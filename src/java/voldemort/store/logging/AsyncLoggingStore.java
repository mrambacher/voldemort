/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.logging;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.DelegatingAsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;

/**
 * A store wrapper that handles debug logging.
 * 
 * 
 */
public class AsyncLoggingStore<K, V, T> extends DelegatingAsynchronousStore<K, V, T> {

    private final Logger logger;
    private final String instanceName;

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     */
    public AsyncLoggingStore(AsynchronousStore<K, V, T> store) {
        this(store, null);
    }

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     * @param instance The instance name to display in logging messages
     * @param time The time implementation to use for computing elapsed time
     */
    public AsyncLoggingStore(AsynchronousStore<K, V, T> store, String instance) {
        super(store);
        this.logger = Logger.getLogger(store.getClass());
        this.instanceName = instance == null ? ": " : instance + ": ";
    }

    @Override
    protected <R> StoreFuture<R> buildFuture(StoreFuture<R> future) {
        future.register(new StoreFutureListener<R>() {

            public void futureFailed(Object operation, VoldemortException ex, long durationNs) {
                printTimedMessage(operation.toString(),
                                  true,
                                  TimeUnit.MILLISECONDS.convert(durationNs, TimeUnit.NANOSECONDS));
            }

            public void futureCompleted(Object operation, R result, long durationNs) {
                printTimedMessage(operation.toString(),
                                  true,
                                  TimeUnit.MILLISECONDS.convert(durationNs, TimeUnit.NANOSECONDS));
            }
        }, future.getOperation());
        return future;

    }

    @Override
    public void close() throws VoldemortException {
        if(logger.isDebugEnabled())
            logger.debug("Closing " + getName() + ".");
        super.close();
    }

    private void printTimedMessage(String operation, boolean success, long elapsedMs) {
        if(logger.isDebugEnabled()) {
            logger.debug(instanceName + operation + " operation on store '" + getName()
                         + "' completed " + (success ? "successfully" : "unsuccessfully") + " in "
                         + elapsedMs + " ms.");
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case LOGGER:
                return this.logger;
            case ASYNCHRONOUS:
                return this;
            default:
                return super.getCapability(capability);
        }
    }

}
