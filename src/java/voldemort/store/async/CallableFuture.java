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

import voldemort.VoldemortException;

/**
 * Callable Futures are store futures that run on the current thread at the time
 * of creation (not asynchronous).
 */
public class CallableFuture<V> extends InlineStoreFuture<V> {

    /**
     * Creates a new Callable Future
     * 
     * @param operation The name of the operation for this future.
     * @param task The task to executefor this future.
     */
    public CallableFuture(String operation, Callable<V> task) {
        super(operation);
        runTask(task);
    }

    private void runTask(Callable<V> task) {
        try {
            result = task.call();
            this.markAsCompleted(result);
        } catch(VoldemortException e) {
            exception = e;
            markAsFailed(e);
        } catch(Exception e) {
            exception = new VoldemortException(e);
            markAsFailed(exception);
        }
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean cancel(boolean interrupt) {
        return false;
    }
}
