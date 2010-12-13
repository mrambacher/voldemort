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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.VoldemortInterruptedException;
import voldemort.store.UnreachableStoreException;

/**
 * Base class for implementing StoreFutures. This class provides a base
 * implementation for creating Store Futures. This class provides much of the
 * basic functionality for derived implementations.
 */
abstract public class StoreFutureTask<V> implements StoreFuture<V> {

    protected final Logger logger = LogManager.getLogger(getClass());

    private final String operation;
    /** Time at which the future was created. */
    protected long submitted = 0;
    /** Time at which the future started. */
    protected long started = 0;
    /** How long the future took to complete. */
    private long duration;
    /** Whether or not the future is complete. */
    private boolean isComplete = false;
    /** Listeners registered for future completion. */
    private Map<StoreFutureListener<V>, Object> listeners;
    /** Countdown latch signaled when the future has completed. */
    protected final CountDownLatch remaining;

    /**
     * Creates a new future task.
     * 
     * @param operation The name of the operation for this future.
     */
    protected StoreFutureTask(String operation) {
        this(operation, 1);
    }

    /**
     * Creates a new future task.
     * 
     * @param operation The name of the operation for this future.
     */
    protected StoreFutureTask(String operation, int count) {
        this.operation = operation;
        this.duration = 0;
        this.submitted = System.nanoTime();
        listeners = new LinkedHashMap<StoreFutureListener<V>, Object>();
        remaining = new CountDownLatch(count);
    }

    /**
     * Returns the name of the operation.
     */
    public String getOperation() {
        return this.operation;
    }

    /**
     * Returns whether or not the future has completed. This method returns true
     * if the task has completed, regardless of if the registered listeners have
     * completed.
     */
    public boolean isDone() {
        try {
            return remaining.await(0, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            return false;
        }
    }

    /**
     * Registers the listener with this future. This method will run the
     * listener immediately if the future has already completed. If the future
     * has not yet completed, then the listener will be invoked when the future
     * completes. Listeners are invoked in the order in which they are
     * registered.
     * 
     * @param listener The listener to be registered.
     * @param arg The argument to pass to listener when invoked.
     */
    public synchronized void register(StoreFutureListener<V> listener, Object arg) {
        if(!isCancelled()) {
            if(!isDone()) {
                listeners.put(listener, arg);
            } else {
                try {
                    V result = getResult();
                    notifyCompletedListener(listener, arg, result);
                } catch(VoldemortException exception) {
                    notifyFailedListener(listener, arg, exception);
                }
            }
        }
    }

    /**
     * Returns the result of the future. This method will return the result of
     * the future or throw an exception, if the future failed.
     * 
     * @return The result of the future
     * @throws VoldemortException if the future failed or has been interrupted.
     */
    abstract protected V getResult() throws VoldemortException;

    /**
     * Returns the result of the future, waiting for it to complete if it is
     * still running. This method will return after the future has completed and
     * all registered listeners have been invoked.
     * 
     * @return The result of the future
     * @throws VoldemortException if the future failed or has been interrupted.
     */
    public V get() throws VoldemortException {
        if(this.isCancelled()) {
            throw new VoldemortInterruptedException("Operation was cancelled");
        } else {
            try {
                remaining.await();
                return getResult();
            } catch(InterruptedException e) {
                throw new VoldemortInterruptedException(e);
            }
        }
    }

    /**
     * Returns the result of the future, waiting for it to complete or the
     * timeout to expire. This method will return after the future has completed
     * and all registered listeners have been invoked.
     * 
     * @param timeout How long to wait for the future to complete.
     * @param units The units of the timeout
     * @return The result of the future
     * @throws VoldemortException if the future failed or has been interrupted.
     */
    public V get(long timeout, TimeUnit units) throws VoldemortException {
        try {
            if(remaining.await(timeout, units)) {
                return getResult();
            } else {
                throw new UnreachableStoreException("Timed out waiting for " + operation + "result");
            }
        } catch(InterruptedException e) {
            throw new VoldemortInterruptedException(e);
        }
    }

    /**
     * Invokes the future completed method for the listener, trapping any
     * exceptions thrown.
     * 
     * @param listener The listener to invoke
     * @param key The key associated with this listener at registration time.
     * @param result The result of the future.
     */
    protected void notifyCompletedListener(StoreFutureListener<V> listener, Object key, V result) {
        try {
            listener.futureCompleted(key, result, duration);
        } catch(Exception e) {}
    }

    /**
     * Notifies all of the registered listeners of the completed result. This
     * method will invoke all of the registered listeners completed method,
     * passing in the result.
     * 
     * @param result The result of the future operation.
     */
    protected void notifyCompletedListeners(V result) {
        for(Map.Entry<StoreFutureListener<V>, Object> entry: listeners.entrySet()) {
            StoreFutureListener<V> listener = entry.getKey();
            notifyCompletedListener(listener, entry.getValue(), result);
        }
    }

    /**
     * Marks the future as completed. This method marks the future as complete,
     * notifies all registered listeners, and decrements the remaining latch,
     * signaling any tasks waiting on the future to complete.
     * 
     * @param result The result of the future operation.
     */
    protected synchronized void markAsCompleted(V result) {
        if(!isComplete) {
            isComplete = true;
            try {
                notifyCompletedListeners(result);
            } finally {
                while(remaining.getCount() > 0) {
                    remaining.countDown();
                }
            }
        } else if(logger.isTraceEnabled()) {
            logger.trace("Ignoring late completion for" + getOperation());
        }
    }

    /**
     * Invokes the future failed method for the listener, trapping any
     * exceptions thrown.
     * 
     * @param listener The listener to invoke
     * @param key The key associated with this listener at registration time.
     * @param ex The exception thrown by the future.
     */
    protected void notifyFailedListener(StoreFutureListener<V> listener,
                                        Object key,
                                        VoldemortException ex) {
        try {
            listener.futureFailed(key, ex, duration);
        } catch(Exception e) {

        }
    }

    /**
     * Notifies all of the registered listeners of the failed result. This
     * method will invoke all of the registered listeners failed method, passing
     * in the exception.
     * 
     * @param ex The exception thrown by the future operation.
     */
    protected void notifyFailedListeners(VoldemortException ex) {
        this.duration = System.nanoTime() - started;
        for(Map.Entry<StoreFutureListener<V>, Object> entry: listeners.entrySet()) {
            StoreFutureListener<V> listener = entry.getKey();
            notifyFailedListener(listener, entry.getValue(), ex);
        }
    }

    /**
     * Marks the future as failed. This method marks the future as failed,
     * notifies all registered listeners, and decrements the remaining latch,
     * signaling any tasks waiting on the future to complete.
     * 
     * @param ex The exception thrown by the future operation.
     */
    protected synchronized void markAsFailed(VoldemortException ex) {
        if(!isComplete) {
            isComplete = true;
            try {
                notifyFailedListeners(ex);
            } finally {
                while(remaining.getCount() > 0) {
                    remaining.countDown();
                }
            }
        } else if(logger.isTraceEnabled()) {
            logger.trace("Ignoring late failure for" + getOperation());
        }
    }

    /**
     * Returns how long the future took to complete. If the future has not
     * started,the duration is since the future was created. If the future has
     * started but not completed, the duration is since the future started.
     * 
     * @param units The time unit to return the duration in.
     */
    public long getDuration(TimeUnit units) {
        if(duration > 0) {
            return units.convert(duration, TimeUnit.NANOSECONDS);
        } else if(started > 0) {
            return units.convert(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        } else {
            return units.convert(System.nanoTime() - submitted, TimeUnit.NANOSECONDS);
        }
    }

    public long getRemaining(long timeout, TimeUnit units) {
        if(isDone() || isCancelled()) {
            return 0;
        } else {
            return Math.max(0, timeout - getDuration(units));
        }
    }
}
