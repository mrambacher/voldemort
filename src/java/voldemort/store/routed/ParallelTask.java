/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.store.routed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortApplicationException;
import voldemort.VoldemortException;
import voldemort.store.InsufficientSuccessfulNodesException;

/**
 * Runs multiple callable
 */
public class ParallelTask<N, V> implements Future<Map<N, V>> {

    // FutureTask to release Semaphore as completed
    private class NodeFuture extends FutureTask<V> {

        private final N node;

        NodeFuture(N node, Callable<V> c) {
            super(c);
            this.node = node;
        }

        @Override
        protected void done() {
            completed.add(this);
            super.done();
        }
    }

    /** The set of jobs started */
    private final List<NodeFuture> submitted;
    /** The jobs completed whose results have not yet been harvested */
    private final BlockingQueue<NodeFuture> completed;
    /** The number of jobs started */
    private final int size;
    /** The name of the jobs */
    private final String taskName;
    /** The number of jobs that have been harvested */
    private int retrieved;
    /** True if the set of jobs has been canceled, false otherwise */
    private boolean cancelled = false;

    /**
     * Creates and starts the set of parallel tasks
     * 
     * @param taskName The name of the task being started
     * @param pool The executor for this request
     * @param callable The set of requests to run.
     */
    public ParallelTask(String taskName, ExecutorService pool, Map<N, Callable<V>> callable) {
        this.taskName = taskName;
        this.retrieved = 0;
        this.size = callable.size();
        this.submitted = new ArrayList<NodeFuture>(size);
        this.completed = new ArrayBlockingQueue<NodeFuture>(size);
        for(Map.Entry<N, Callable<V>> entry: callable.entrySet()) {
            NodeFuture f = new NodeFuture(entry.getKey(), entry.getValue());
            this.submitted.add(f);
        }

        for(NodeFuture f: submitted) {
            pool.execute(f);
        }
    }

    /**
     * Creates and starts the set of parallel tasks
     * 
     * @param taskName The name of the task being started
     * @param pool The executor for this request
     * @param callable The set of requests to run.
     */
    public static <S, T> ParallelTask<S, T> newInstance(String taskName,
                                                        ExecutorService pool,
                                                        Map<S, Callable<T>> tasks) {
        return new ParallelTask<S, T>(taskName, pool, tasks);
    }

    /**
     * Cancels the tasks associated with this job.
     */
    public boolean cancel(boolean mayInterruptIfRunning) {
        if(this.isDone()) {
            return false;
        }
        this.cancelled = true;
        for(Future<V> f: this.submitted) {
            f.cancel(mayInterruptIfRunning);
        }
        return this.cancelled;
    }

    /**
     * Returns the status of the remaining tasks, blocking until they finish
     */
    public Map<N, V> get() {
        Map<N, V> results = new HashMap<N, V>(size);
        boolean done = false;
        try {
            while(this.retrieved < this.size) { // If there are more jobs
                if(this.isCancelled()) { // And the tasks are not canceled
                    break;
                }
                NodeFuture future = getNextResult(); // Get the next completed
                // task
                if(future != null) { // If there was one
                    try {
                        V result = getResult(future); // Get the result of the
                        // task
                        results.put(future.node, result); // Store the result
                    } catch(VoldemortException e) { // Job failed
                        throw e; // Throw the failure
                    }
                }
            }
            done = true;
        } finally {
            if(!done)
                this.cancel(true);
        }
        return results;
    }

    /**
     * Returns the status of the next sub-task, blocking until one finishes
     */
    NodeFuture getNextResult() {
        NodeFuture future = null;
        if(this.retrieved < this.size) {
            try {
                future = completed.take();
            } catch(InterruptedException e) {
                return null;
            }
            if(future != null) {
                this.retrieved++;
            }
        }
        return future;

    }

    /**
     * Returns the status of the next sub-task, blocking until one finishes
     * 
     * @param waitTime How long to wait for the task to complete
     * @returns Returns the completed task or null on timeout
     */
    NodeFuture getNextResult(long waitTime, TimeUnit units) {
        NodeFuture future = null;
        if(this.retrieved < this.size) {
            try {
                future = completed.poll(waitTime, units);
            } catch(InterruptedException e) {
                return null;
            }
            if(future != null) {
                retrieved++;
            }
        }
        return future;
    }

    /**
     * Retrieves the result from the future operation
     * 
     * @param future The completed future
     * @return The result of the future
     * @throws VoldemortException The exception thrown from the future
     */
    protected V getResult(NodeFuture future) throws VoldemortException {
        try {
            return future.get();
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        } catch(ExecutionException e) {
            Throwable cause = e.getCause();
            if(cause instanceof VoldemortException) {
                throw (VoldemortException) cause;
            } else {
                throw new VoldemortException(cause);
            }
        }
    }

    /**
     * Waits for either the timeout to expire or a number of tasks to complete.
     * 
     * @param preferred The preferred number of successful tasks to wait for.
     * @param required The number required for a successful return
     * @param timeout How long to wait for the tasks to complete
     * @param unit The unit of the timeout
     * @return The set of results for the successful nodes
     * @throws InsufficientSuccessfulNodesException, if fewer than required
     *         nodes completed successfully.
     */
    public Map<N, V> get(int preferred, int required, long timeout, TimeUnit unit) {
        List<VoldemortException> exceptions = new ArrayList<VoldemortException>();
        Map<N, V> results = get(preferred, timeout, unit, exceptions);
        if(results.size() < required) {
            throw new InsufficientSuccessfulNodesException(taskName + ": Required " + required
                                                                   + " but only " + results.size()
                                                                   + " succeeded",
                                                           exceptions,
                                                           this.size,
                                                           required,
                                                           results.size());
        }
        return results;
    }

    /**
     * Retrieves the results of the tasks that completed within the timeout
     * period
     * 
     * @param timeout How long to wait
     * @param unit The unit of the timeout
     * @return The results of successfully completed tasks.
     */
    public Map<N, V> get(long timeout, TimeUnit unit) {
        return get(this.size, timeout, unit, null);
    }

    /**
     * Retrieves the results of the tasks that completed within the timeout
     * period This method waits for either the preferred number of tasks to
     * complete or the timeout period to expire.
     * 
     * @param preferred How many tasks to wait for
     * @param timeout How long to wait
     * @param unit The unit of the timeout
     * @return The results of successfully completed tasks.
     */
    public Map<N, V> get(int preferred,
                         long timeout,
                         TimeUnit unit,
                         List<VoldemortException> exceptions) {
        // timeout handling isn't perfect, but it's an attempt to
        // replicate the behavior found in AbstractExecutorService
        Map<N, V> results = new HashMap<N, V>(preferred);

        long start = System.nanoTime();
        long nanoTimeout = unit.toNanos(timeout);
        while(results.size() < preferred) {
            if(this.isCancelled()) {
                break;
            }
            long current = System.nanoTime();
            long elapsed = current - start;
            if(elapsed >= nanoTimeout) {
                break;
            }
            NodeFuture f = getNextResult(nanoTimeout - elapsed, TimeUnit.NANOSECONDS);
            if(f != null) {
                try {
                    V result = getResult(f);
                    results.put(f.node, result);
                } catch(VoldemortApplicationException e) {
                    throw e;
                } catch(VoldemortException e) {
                    if(exceptions != null) {
                        exceptions.add(e);
                        // If the number of failures precludes the operation
                        // from being successful,
                        // stop waiting for more successful results, as we
                        // cannot reach the number of
                        // preferred operations (there are too few jobs left to
                        // reach that level).
                        if(preferred > this.size - exceptions.size()) {
                            break;
                        }
                    }
                }
            }
        }
        // OK, at this point, we either have the correct number of results or
        // have timed out.
        // Now, check if there are any results left (without blocking)
        for(NodeFuture f = getNextResult(0, unit); f != null; f = getNextResult(0, unit)) {
            try {
                V result = getResult(f);
                results.put(f.node, result);
            } catch(VoldemortApplicationException e) {
                throw e;
            } catch(VoldemortException e) {
                if(exceptions != null) {
                    exceptions.add(e);
                }
            }
        }
        return results;
    }

    /**
     * Returns true if the jobs were canceled.
     */
    public boolean isCancelled() {
        return this.cancelled;
    }

    /**
     * Returns the number of tasks that have not been collected. These tasks are
     * either still running or have completed but not retrieved
     * 
     * @return The number of tasks still remaining.
     */
    public int getRemaining() {
        return completed.size() + size - retrieved;
    }

    /**
     * Returns true if all subtasks associated with this job have completed.
     */
    public boolean isDone() {
        return (this.retrieved + completed.size()) >= this.size;
    }
}