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

package voldemort.client;

public class VoldemortInterruptedException extends VoldemortClientException {

    private static final long serialVersionUID = 1L;

    public VoldemortInterruptedException(String s, Throwable t) {
        super(s, t);
    }

    public VoldemortInterruptedException(String s) {
        super(s);
    }

    public VoldemortInterruptedException(Throwable t) {
        super(t);
    }
}
