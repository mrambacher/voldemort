package voldemort.store.distributed;

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
import voldemort.store.async.StoreFutureTask;

abstract public class AbstractDistributedFuture<N, V> extends StoreFutureTask<V> implements
        DistributedFuture<N, V> {

    /** The class used to collate the results */
    private final ResultsBuilder<N, V> responseBuilder;
    protected boolean cancelled = false;
    /** The number of futures to wait for */
    protected final int preferred;
    /** The number of futures that must be successful for this one to succeed. */
    protected final int required;
    /** The number of potential future operations. */
    protected final int available;
    /** The number of futures still running */
    protected final CountDownLatch running;
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
     * 
     * @param operation The name of the operation for this future (e.g. "GET")
     * @param futures The set of futures upon which to wait for completion.
     * @param builder Class used to build the results returned by this future.
     * @param preferred How many futures to wait for completion of.
     * @param required How many futures must complete successfully for this
     *        future to be successful.
     */
    public AbstractDistributedFuture(String operation,
                                     ResultsBuilder<N, V> builder,
                                     int available,
                                     int preferred,
                                     int required) {
        super(operation, required);
        this.required = required;
        this.preferred = preferred;
        this.available = available;
        this.started = this.submitted;
        this.responseBuilder = builder;
        this.running = new CountDownLatch(available);
        this.successes = new AtomicInteger(0);
        this.failures = new AtomicInteger(0);
        results = new ConcurrentHashMap<N, V>();
        exceptions = new ConcurrentHashMap<N, VoldemortException>();
    }

    public int getAvailable() {
        return available;
    }

    public int getRequired() {
        return required;
    }

    public int getPreferred() {
        return preferred;
    }

    /**
     * Returns true if this future has completed successfully.
     */
    public boolean isSuccessful() {
        return (successes.intValue() >= this.required);
    }

    /**
     * Returns the result for this distributed future. If enough futures
     * completed successfully, this method will return the collated results
     * collected so far.
     * 
     * @return The collated results
     * @throws VoldemortinterruptedException If the future was cancelled
     * @throws VoldemortApplicationException If any submitted future threw an
     *         application exception
     * @throws InsufficientSuccessfulNodesException If too few futures completed
     *         successfully
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
                                                               available,
                                                               required,
                                                               results.size());
            }
        }
    }

    /**
     * Waits for the distributed future to complete and returns the result. This
     * method waits for either the future to complete or the timeout to expire.
     * When either of those results occur, this method checks to see if enough
     * results have been obtained to return.
     * 
     * @param timeout How long to wait for the future to complete
     * @param units The units for the timeout.
     * @return The result for this future or an exception
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
     * 
     * @param stopIfRunning Whether or not to stop the future if it is already
     *        running.
     * @return true if the future was cancelled, false otherwise.
     */
    public boolean cancel(boolean stopIfRunning) {
        if(isDone()) {
            return false;
        } else if(!cancelled) {
            for(N node: this.getNodes()) {
                StoreFuture<V> future = getFuture(node);
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
     * 
     * @param node The node to query
     * @param units The units the duration should be returned in.
     * @return How long the future for the input node took to complete.
     */
    public long getDuration(N node, TimeUnit units) {
        StoreFuture<V> future = getFuture(node);
        return future.getDuration(units);
    }

    /**
     * Waits for all running futures associated with this future to complete.
     * 
     * @return the map of results for this future.
     * @throws VoldemortInterruptedException If the tasks was interrupted.
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
     * 
     * @return the map of results for this future.
     * @param timeout How long to wait for the sub-tasks to complete
     * @param units The units for the timeout.
     * @throws VoldemortInterruptedException If the tasks was interrupted.
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
                                                                   available,
                                                                   available,
                                                                   results.size());

                }
            } catch(InterruptedException e) {
                throw new VoldemortInterruptedException(e);
            }
        }
    }

    /**
     * Returns the number of futures that have completed (either successfulyl or
     * failed).
     */
    public int getCompleted() {
        return results.size() + exceptions.size();
    }

    /**
     * Returns a map of the results (result for each node) collected for this
     * future so far.
     */
    public Map<N, V> getResults() {
        return results;
    }

    /**
     * Returns a map of the exceptions (exceptions for each node) collected for
     * this future so far.
     */
    public Map<N, VoldemortException> getExceptions() {
        return this.exceptions;
    }

    /**
     * Returns true if the future for the given node has completed.
     * 
     * @param node The node being queried
     * @return true if the future associated with the node has already
     *         completed, false otherwise.
     */
    public boolean isDone(N node) {
        if(results.containsKey(node) || exceptions.containsKey(node)) {
            return true;
        } else {
            StoreFuture<V> future = getFuture(node);
            return future.isDone();
        }
    }

    /**
     * Returns true if the future has completed
     * 
     * @return True if the future has completed, false if any of the sub-tasks
     *         are still running.
     */
    public boolean isComplete() {
        return (getCompleted() == available);
    }

    /**
     * Collects the successes from the nested futures. If enough successes, then
     * build the results and notify the listeners
     * 
     * @param key The node registered for this future
     * @param result The result of this future
     * @param duration How long that future took
     */
    protected synchronized void nodeCompleted(N node, V result, long duration) {
        try {
            results.put(node, result);
            int successCount = successes.incrementAndGet();
            if(successCount == preferred) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with " + successCount + "/"
                                 + preferred + " successes");
                }
                super.markAsCompleted(getResult());
            } else if(available == successCount + failures.intValue()) {
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
    protected synchronized void nodeFailed(N node, VoldemortException exception, long duration) {
        try {
            this.exceptions.put(node, exception);
            int failureCount = failures.incrementAndGet();

            // If we get an application exception, quit immediately
            if(exception instanceof VoldemortApplicationException) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with application exception "
                                 + exception.getMessage());
                }
                super.markAsFailed(exception);
            } else if(required == available - failureCount + 1) {
                // If there are not enough responses left,then throw an
                // exception
                if(logger.isDebugEnabled()) {
                    logger.debug("Completing " + getOperation() + " with " + failureCount
                                 + " failures");
                }
                super.markAsFailed(new InsufficientSuccessfulNodesException("Too few successful nodes for operation",
                                                                            exceptions.values(),
                                                                            available,
                                                                            required,
                                                                            results.size()));
            } else if(available == failureCount + successes.intValue()) {
                // If there are no responses left, then we need to mark the task
                // as complete
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
