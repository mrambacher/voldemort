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

import voldemort.VoldemortException;

abstract public class InlineStoreFuture<V> extends StoreFutureTask<V> {

    protected V result;
    protected VoldemortException exception;

    /**
     * Creates a new Callable Future
     * 
     * @param operation The name of the operation for this future.
     * @param task The task to executefor this future.
     */
    public InlineStoreFuture(String operation) {
        super(operation);
    }

    @Override
    protected V getResult() throws VoldemortException {
        if(exception != null) {
            throw exception;
        } else {
            return result;
        }
    }

}
