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

package voldemort.store.failuredetector;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.DelegatingAsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;
import voldemort.utils.Time;

/**
 * A Store which monitors requests and changes the state of the nodes.
 */
public class FailureDetectingStore<K, V> extends DelegatingAsynchronousStore<K, V> {

    private final Node node;
    private final FailureDetector failureDetector;

    public static <K, V> Store<K, V> create(Node node, FailureDetector detector, Store<K, V> store) {
        AsynchronousStore<K, V> async = AsyncUtils.asAsync(store);
        return AsyncUtils.asSync(create(node, detector, async));
    }

    public static <K, V> AsynchronousStore<K, V> create(Node node,
                                                        FailureDetector detector,
                                                        AsynchronousStore<K, V> async) {
        return new FailureDetectingStore<K, V>(node, detector, async);
    }

    /**
     * Creates a new node store
     * 
     * @param node The node being used
     * @param detector The failure detector for this node
     * @param inner The underlying store.
     */
    public FailureDetectingStore(Node node, FailureDetector detector, AsynchronousStore<K, V> inner) {
        super(inner);
        this.node = node;
        this.failureDetector = detector;
    }

    @Override
    protected <R> StoreFuture<R> buildFuture(StoreFuture<R> future) {
        future.register(new StoreFutureListener<R>() {

            public void futureFailed(Object handle, VoldemortException ex, long durationNs) {
                if(ex instanceof UnreachableStoreException)
                    recordException(durationNs, (UnreachableStoreException) ex);
            }

            public void futureCompleted(Object handle, R result, long durationNs) {
                recordSuccess(durationNs);
            }
        }, null);
        return future;

    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case FAILURE_DETECTOR:
                return this.failureDetector;
            case ASYNCHRONOUS:
                return this;
            default:
                return super.getCapability(capability);
        }
    }

    /**
     * Records an exception for a given node
     * 
     * @param startNs The time at which the operation started
     * @param e The caught exception
     */
    private void recordException(long startNs, UnreachableStoreException e) {
        failureDetector.recordException(node, (System.nanoTime() - startNs) / Time.NS_PER_MS, e);
    }

    /**
     * Records a successful operation for a given node
     * 
     * @param startNs The time at which the operation started
     */
    private void recordSuccess(long startNs) {
        failureDetector.recordSuccess(node, (System.nanoTime() - startNs) / Time.NS_PER_MS);
    }

    /**
     * Returns true if the node is currently available
     * 
     * @return True if the node is available, false otherwise.
     */
    public boolean isAvailable() {
        return this.failureDetector.isAvailable(node);
    }

    /**
     * Throws an exception if the node is currently not available.
     */
    private void checkNodeIsAvailable() {
        if(!isAvailable()) {
            throw new UnreachableStoreException("Node is down");
        }
    }
}
