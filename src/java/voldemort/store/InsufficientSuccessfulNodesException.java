/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store;

import java.util.Collection;

/**
 * Thrown if an operation fails due to too few reachable nodes.
 * 
 * @author jay
 * 
 */
public class InsufficientSuccessfulNodesException extends InsufficientOperationalNodesException {

    private static final long serialVersionUID = 1L;
    private int successes;

    public InsufficientSuccessfulNodesException(String s) {
        this(s, -1, -1, -1);
    }

    public InsufficientSuccessfulNodesException(String message,
                                                int available,
                                                int required,
                                                int successes) {
        super(message, available, required);

        setSuccessful(successes);
    }

    public InsufficientSuccessfulNodesException(String message,
                                                Collection<? extends Throwable> failures,
                                                int available,
                                                int required,
                                                int successes) {
        super(message, failures, available, required);

        setSuccessful(successes);
    }

    public void setSuccessful(int i) {
        successes = i;
    }

    public int getSuccessful() {
        return successes;
    }

}
