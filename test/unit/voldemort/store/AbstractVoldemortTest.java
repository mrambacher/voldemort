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
import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

public class AbstractVoldemortTest<V> extends TestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected boolean valuesEqual(V t1, V t2) {
        return Objects.equal(t1, t2);
    }

    protected void assertEquals(Versioned<V> v1, Versioned<V> v2) {
        assertEquals(null, v1, v2);
    }

    protected void assertEquals(String message, Versioned<V> v1, Versioned<V> v2) {
        String assertTrueMessage = v1 + " != " + v2 + ".";
        if(message != null)
            assertTrueMessage += message;
        assertTrue(assertTrueMessage, valuesEqual(v1.getValue(), v2.getValue()));
        assertEquals(message, v1.getVersion(), v2.getVersion());
        assertEquals(message, v1.getMetadata(), v2.getMetadata());
    }

    public void assertContains(Collection<Versioned<V>> collection, Versioned<V> value) {
        boolean found = false;
        for(Versioned<V> t: collection)
            if(valuesEqual(t.getValue(), value.getValue()))
                found = true;
        assertTrue(collection + " does not contain " + value + ".", found);
    }

    protected void assertContainsVersioned(String message,
                                           Versioned<V> expected,
                                           List<Versioned<V>> found) {
        // check that there is a single remaining version, namely the
        // non-deleted
        assertEquals(1, found.size());
        assertEquals(message, expected, found.get(0));
    }
}
