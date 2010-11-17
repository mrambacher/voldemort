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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.VoldemortApplicationException;
import voldemort.VoldemortException;
import voldemort.client.VoldemortInterruptedException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;
import voldemort.store.async.StoreFutureTask;

/**
 * Implementation class for a basic distributed future.
 * This future task takes in a collection of futures and a builder.  This future waits for "preferred" futures
 * to completes but returns success if "required" futures completed successfully.
 * */
public class DistributedFutureTask<N, V> extends StoreFutureTask<V> implements
        DistributedFuture<N, V>, StoreFutureListener<V> {

    protected boolean cancelled = false;
    /** The number of futures to wait for */
    protected final int preferred;
    /** The number of futures that must be successful for this one to succeed.  */
    protected final int required;
    /** The number of futures still running */
    protected final CountDownLatch running;
    /** The class used to collate the results */
    private final ResultsBuilder<N, V> responseBuilder;
    /** The set of futures associated with this one. */
    private final Map<N, StoreFuture<V>> submitted;
    /** The set of results received from the submitted futures. */
    private Map<N, V> results;
    /** The set of exceptions received from the submitted futures. */
    private Map<N, VoldemortException> exceptions;
    /** The number of futures that have completed successfully. */
    private AtomicInteger successes;
    /** The number of futures that have failed. */
    private AtomicInteger failures;

    /**
     * Creates a new distributed future task
     * @param operation     The name of the operation for this future (e.g. "GET")
     * @param futures       The set of futures upon which to wait for completion.
     * @param builder       Class used to build the results returned by this future.
     * @param preferred     How many futures to wait for completion of.
     * @param required      How many futures must complete successfully for this future to be successful.
     */
    public DistributedFutureTask(String operation,
                                 Map<N, StoreFuture<V>> futures,
                                 ResultsBuilder<N, V> builder,
                                 int preferred,
                                 int required) {
        super(operation, required);
        this.submitted = futures;
        this.responseBuilder = builder;
        this.required = required;
        this.preferred = preferred;
        this.running = new CountDownLatch(submitted.size());
        this.successes = new AtomicInteger(0);
        this.failures = new AtomicInteger(0);
        results = new ConcurrentHashMap<N, V>();
        exceptions = new ConcurrentHashMap<N, VoldemortException>();
        for(Map.Entry<N, StoreFuture<V>> entry: submitted.entrySet()) {
            StoreFuture<V> future = entry.getValue();
            future.register(this, entry.getKey());
        }
    }

    /**
     * Returns true if this future has completed successfully. 
     */
    public boolean isSuccessful() {
        return (successes.intValue() >= this.required);
    }

    /** 
     * Returns the result for this distributed future.
     * If enough futures completed successfully, this method will return the collated results collected so far.
     * 
     * @return  The collated results 
     * @throws VoldemortinterruptedException    If the future was cancelled
     * @throws VoldemortApplicationException    If any submitted future threw an application exception
     * @throws InsufficientSuccessfulNodesException If too few futures completed successfully
     */
    @Override
    protected V getResult() throws VoldemortException {
        if(isCancelled()) {
            throw new VoldemortInterruptedException("Operation interrupted prior to completion");
        } else {
            // Check to see if we have any application exceptions.
            // If so, throw them through
            for(VoldemortException ex: exceptions.values()) {
                if(ex instanceof VoldemortApplicationException) {
                    throw ex;
                }
            }
            if(results.size() >= this.required) {
                return responseBuilder.buildResponse(results);
            } else {
                logger.info("Too few successful nodes for " + getOperation() + "; required "
                            + required + " but only " + results.size() + " successes");
                throw new InsufficientSuccessfulNodesException("Too few successful nodes for "
                                                                       + getOperation(),
                                                               exceptions.values(),
                                                               submitted.size(),
                                                               required,
                                                               results.size());
            }
        }
    }

    /**
     * Waits for the distributed future to complete and returns the result.
     * This method waits for either the future to complete or the timeout to expire.  
     * When either of those results occur, this method checks to see if enough results have
     * been obtained to return.
     * @param timeout       How long to wait for the future to complete
     * @param units         The units for the timeout.
     * @return              The result for this future or an exception 
     * @see getResult
     * @throws VoldemortInterruptedException if an interrupt was trapped
     */
    @Override
    public V get(long timeout, TimeUnit units) throws VoldemortException {
        if(isCancelled()) {
            throw new VoldemortInterruptedException("Operation was cancelled");
        } else {
            try {
                if(remaining.await(timeout, units)) {
                    return getResult();
                } else {
                    return getResult();
                }
            } catch(InterruptedException e) {
                throw new VoldemortInterruptedException(e);
            }
        }
    }

    /**
     * Cancels this future and its related tasks.
     * @param stopIfRunning     Whether or not to stop the future if it is already running.
     * @return  true if the future was cancelled, false otherwise.
     */
    public boolean cancel(boolean stopIfRunning) {
        if(isDone()) {
            return false;
        } else if(!cancelled) {
            for(StoreFuture<V> future: submitted.values()) {
                if(future.cancel(stopIfRunning)) {
                    cancelled = true;
                }
            }
        }
        return this.cancelled;
    }

    /**
     * Returns true if the jobs were canceled.
     */
    public boolean isCancelled() {
        return this.cancelled;
    }

    /**
     * Returns how long the future for the given node took to complete.
     * @param node      The node to query
     * @param units     The units the duration should be returned in.
     * @return          How long the future for the input node took to complete.
     */
    public long getDuration(N node, TimeUnit units) {
        StoreFuture<V> future = submitted.get(node);
        return future.getDuration(units);
    }

    /**
     * Waits for all running futures associated with this future to complete.
     * @return the map of results for this future.
     * @throws VoldemortInterruptedException    If the tasks was interrupted.
     */
    public Map<N, V> complete() throws VoldemortException {
        if(isCancelled()) {
            throw new VoldemortInterruptedException("Operation was cancelled");
        } else {
            try {
                running.await();
                return getResults();
            } catch(InterruptedException e) {
                throw new VoldemortInterruptedException(e);
            }
        }
    }

    /**
     * Waits for all running futures associated with this future to complete.
     * @return the map of results for this future.
     * @param timeout   How long to wait for the sub-tasks to complete
     * @param units     The units for the timeout.
     * @throws VoldemortInterruptedException    If the tasks was interrupted.
     */
    public Map<N, V> complete(long timeout, TimeUnit units) throws VoldemortException {
        if(isCancelled()) {
            throw new VoldemortInterruptedException("Operation was cancelled");
        } else {
            try {
                if(running.await(timeout, units)) {
                    return getResults();
                } else {
                    throw new InsufficientSuccessfulNodesException("Too few successful nodes for operation",
                                                                   exceptions.values(),
                                                                   submitted.size(),
                                                                   submitted.size(),
                                                                   results.size());

                }
            } catch(InterruptedException e) {
                throw new VoldemortInterruptedException(e);
            }
        }
    }

    /**
     * Returns the number of futures that have completed (either successfulyl or failed).
     */
    public int getCompleted() {
        return results.size() + exceptions.size();
    }

    /**
     * Returns the set of nodes that are associated with this future.
     */
    public Collection<N> getNodes() {
        return submitted.keySet();
    }

    /**
     * Returns a map of the results (result for each node) collected for this future so far.
     */
    public Map<N, V> getResults() {
        return results;
    }

    /**
     * Returns true if the future for the given node has completed.
     * @param node      The node being queried
     * @return  true if the future associated with the node has already completed, false otherwise.
     */
    public boolean isDone(N node) {
        if(results.containsKey(node) || exceptions.containsKey(node)) {
            return true;
        } else {
            StoreFuture<V> future = submitted.get(node);
            return future.isDone();
        }
    }

    /**
     * Returns true if the future has completed
     * @return  True if the future has completed, false if any of the sub-tasks are still running.
     */
    public boolean isComplete() {
        return (getCompleted() == submitted.size());
    }

    /**
     * Collects the successes from the nested futures. If enough successes, then
     * build the results and notify the listeners
     * 
     * @param key The node registered for this future
     * @param result The result of this future
     * @param duration How long that future took
     */
    @SuppressWarnings("unchecked")
    public synchronized void futureCompleted(Object key, V result, long duration) {
        try {
            N node = (N) key;
            results.put(node, result);
            int successCount = successes.incrementAndGet();
            if(successCount == preferred) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with " + successCount + "/"
                                 + preferred + " successes");
                }
                super.markAsCompleted(getResult());
            } else if(submitted.size() == successCount + failures.intValue()) {
                // If there are no responses left, then we need to mark the task
                // as complete
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with " + successCount
                                 + " successes and " + failures.intValue() + " failures");
                }
                try {
                    super.markAsCompleted(getResult());
                } catch(VoldemortException ex) {
                    // Not enough successful, throw an exception
                    super.markAsFailed(ex);
                }
            }
        } catch(Exception e) {

        }
        running.countDown();
    }

    /**
     * Collects the failures from the nested futures. If enough failures, then
     * notify the listeners
     * 
     * @param key The node registered for this future
     * @param result The result of this future
     * @param duration How long that future took
     */
    @SuppressWarnings("unchecked")
    public synchronized void futureFailed(Object key, VoldemortException exception, long duration) {
        try {
            N node = (N) key;
            this.exceptions.put(node, exception);
            int failureCount = failures.incrementAndGet();

            // If we get an application exception, quit immediately
            if(exception instanceof VoldemortApplicationException) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with application exception "
                                 + exception.getMessage());
                }
                super.markAsFailed(exception);
            } else if(required == submitted.size() - failureCount + 1) {
                // If there are not enough responses left,then throw an exception
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with " + failureCount
                                 + " failures");
                }
                super.markAsFailed(new InsufficientSuccessfulNodesException("Too few successful nodes for operation",
                                                                            exceptions.values(),
                                                                            submitted.size(),
                                                                            required,
                                                                            results.size()));
            } else if(submitted.size() == failureCount + successes.intValue()) {
                // If there are no responses left, then we need to mark the task as complete
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with " + failureCount
                                 + " failures and " + successes.intValue() + " successes");
                }
                try {
                    super.markAsCompleted(getResult());
                } catch(VoldemortException ex) {
                    // Not enough successful, throw an exception
                    super.markAsFailed(ex);
                }
            } else {

            }
        } catch(Exception e) {}
        running.countDown();
    }
}
