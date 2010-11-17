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

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import voldemort.VoldemortException;
import voldemort.client.VoldemortInterruptedException;

/**
 * Class implementing an asynchronous, threadable store future. This class is
 * used by the (@link ThreadedStore) store for future it returns.
 */
public class RunnableFutureTask<V> extends StoreFutureTask<V> implements Runnable {

    private final FutureTask<V> future;

    /**
     * Creates a new StoreFutureTask
     * 
     * @param operation The name of the operation for this future
     * @param c The task to invoke for this future.
     */
    RunnableFutureTask(String operation, Callable<V> callable) {
        super(operation);
        future = new FutureTask<V>(callable);
    }

    /**
     * Returns the result of the future. If an exception was thrown by the
     * future, the exception will be translated into a Voldemort exception
     */
    @Override
    protected V getResult() throws VoldemortException {
        try {
            return future.get();
        } catch(VoldemortException e) {
            throw e;
        } catch(InterruptedException e) {
            throw new VoldemortInterruptedException(e);
        } catch(CancellationException e) {
            throw new VoldemortInterruptedException(e);
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
     * Runs the future task. This method is invoked when the thread starts
     * executing the future.
     */
    public void run() {
        started = System.nanoTime();
        future.run();
        try {
            V result = getResult();
            this.markAsCompleted(result);
        } catch(VoldemortException e) {
            this.markAsFailed(e);
        }
    }

    /**
     * Cancels the future.
     */
    public boolean cancel(boolean interruptIfRunning) {
        return future.cancel(interruptIfRunning);
    }

    /**
     * Returns whether or not the future is canceled.
     */
    public boolean isCancelled() {
        return future.isCancelled();
    }

}
