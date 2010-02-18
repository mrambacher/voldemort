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
import java.util.Collections;

/**
 * Thrown if an operation fails due to too few reachable nodes.
 * 
 * @author jay
 * 
 */
public class InsufficientOperationalNodesException extends StoreOperationFailureException {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Throwable> causes;
    private int available;
    private int required;

    public InsufficientOperationalNodesException(String s) {
        this(s, -1, -1);
    }

    public InsufficientOperationalNodesException(String s, int available, int required) {
        super(s);
        causes = Collections.emptyList();
        setAvailable(available);
        setRequired(required);
    }

    public InsufficientOperationalNodesException(String s, Throwable e) {
        this(s, e, -1, -1);
    }

    public InsufficientOperationalNodesException(String s, Throwable e, int available, int required) {
        super(s, e);
        causes = Collections.singleton(e);
        setAvailable(available);
        setRequired(required);
    }

    public InsufficientOperationalNodesException(Throwable e, int available, int required) {
        super(e);
        causes = Collections.singleton(e);
        setAvailable(available);
        setRequired(required);
    }

    public InsufficientOperationalNodesException(String message,
                                                 Collection<? extends Throwable> failures) {
        this(message, failures, -1, -1);
    }

    public InsufficientOperationalNodesException(String message,
                                                 Collection<? extends Throwable> failures,
                                                 int available,
                                                 int required) {
        super(message, failures.size() > 0 ? failures.iterator().next() : null);

        this.causes = failures;
        setAvailable(available);
        setRequired(required);
    }

    public void setAvailable(int i) {
        available = i;
    }

    public void setRequired(int i) {
        required = i;
    }

    public int getAvailable() {
        return available;
    }

    public int getRequired() {
        return required;
    }

    public Collection<? extends Throwable> getCauses() {
        return this.causes;
    }
}
