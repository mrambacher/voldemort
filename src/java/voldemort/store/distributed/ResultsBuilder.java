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

import java.util.Map;

import voldemort.VoldemortException;

/**
 * Interface used for collecting results from a distributed store.
 */

public interface ResultsBuilder<N, V> {

    /**
     * Method to collect the responses into a single return value
     * 
     * @param responses The set of responses
     * @return The single unified response
     */
    public V buildResponse(Map<N, V> responses) throws VoldemortException;
}
