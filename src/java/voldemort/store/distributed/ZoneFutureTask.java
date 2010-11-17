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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.VoldemortException;
import voldemort.client.VoldemortInterruptedException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;
import voldemort.store.async.StoreFutureTask;

import com.google.common.collect.Maps;

/**
 * Implementation class for a distributed zone future.
 */
public class ZoneFutureTask<N, V> extends StoreFutureTask<V> implements DistributedFuture<N, V>,
        StoreFutureListener<V> {

    private final int preferred;
    private final int required;
    private AtomicInteger successes;
    private AtomicInteger failures;
    protected boolean cancelled = false;
    private final CountDownLatch running;
    private final ResultsBuilder<N, V> responseBuilder;
    private final Collection<DistributedFuture<N, V>> submitted;

    public ZoneFutureTask(String operation,
                          Collection<DistributedFuture<N, V>> futures,
                          ResultsBuilder<N, V> builder,
                          int preferred,
                          int required) {
        super(operation, required);
        this.submitted = futures;
        this.responseBuilder = builder;
        this.required = required;
        this.preferred = preferred;
        this.successes = new AtomicInteger(0);
        this.failures = new AtomicInteger(0);
        this.running = new CountDownLatch(submitted.size());
    }

    public int getCompleted() {
        int completed = 0;
        for(DistributedFuture<N, V> future: submitted) {
            if(future.isDone()) {
                completed++;
            }
        }
        return completed;
    }

    public Map<N, V> getResults() {
        Map<N, V> results = Maps.newHashMap();
        for(DistributedFuture<N, V> future: submitted) {
            results.putAll(future.getResults());
        }
        return results;
    }

    public boolean isDone(N node) {
        for(DistributedFuture<N, V> future: submitted) {
            if(future.getNodes().contains(node)) {
                return future.isDone(node);
            }
        }
        return false;
    }

    public long getDuration(N node, TimeUnit units) {
        for(DistributedFuture<N, V> future: submitted) {
            if(future.getNodes().contains(node)) {
                return future.getDuration(units);
            }
        }
        return 0;
    }

    @Override
    protected V getResult() throws VoldemortException {
        if(successes.intValue() >= preferred) {
            return responseBuilder.buildResponse(this.getResults());
        } else {
            throw new InsufficientZoneResponsesException("Insufficient zones for operation "
                                                         + getOperation());
        }
    }

    @Override
    public V get(long timeout, TimeUnit units) throws VoldemortException {
        if(isCancelled()) {
            throw new VoldemortInterruptedException("Operation was cancelled");
        } else {
            try {
                if(remaining.await(timeout, units)) {
                    return responseBuilder.buildResponse(this.getResults());
                } else {
                    return getResult();
                }
            } catch(InterruptedException e) {
                throw new VoldemortInterruptedException(e);
            }
        }
    }

    public boolean cancel(boolean stopIfRunning) {
        if(isDone()) {
            return false;
        } else if(!cancelled) {
            for(StoreFuture<V> future: submitted) {
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

    public Collection<N> getNodes() {
        Set<N> nodes = new HashSet<N>();
        for(DistributedFuture<N, V> future: submitted) {
            nodes.addAll(future.getNodes());
        }
        return nodes;
    }

    public boolean isSuccessful() {
        return (successes.get() >= this.required);
    }

    public Map<N, V> complete() throws VoldemortException {
        try {
            running.await();
            return getResults();
        } catch(InterruptedException e) {
            throw new VoldemortInterruptedException(e);
        }
    }

    public Map<N, V> complete(long timeout, TimeUnit units) throws VoldemortException {
        try {
            if(running.await(timeout, units)) {
                return getResults();
            } else {
                throw new InsufficientZoneResponsesException("Operation complete timed out waiting for responses");
            }
        } catch(InterruptedException e) {
            throw new VoldemortInterruptedException(e);
        }
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
    public void futureCompleted(Object key, V result, long duration) {
        if(successes.incrementAndGet() == preferred) {
            super.markAsCompleted(getResult());
        } else {
            remaining.countDown();
        }
        running.countDown();
    }

    @SuppressWarnings("unchecked")
    public void futureFailed(Object key, VoldemortException exception, long duration) {
        if(required > submitted.size() - failures.incrementAndGet()) {
            // If there are not enough responses left,then throw an exception
            super.markAsFailed(new InsufficientZoneResponsesException("Too few successful zones for operation"));
        }
        running.countDown();
    }
}
