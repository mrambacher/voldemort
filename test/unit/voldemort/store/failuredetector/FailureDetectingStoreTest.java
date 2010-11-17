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
package voldemort.store.failuredetector;

import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.store.FailingStore;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.AsyncUtils;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

@RunWith(Parameterized.class)
public class FailureDetectingStoreTest extends TestCase {

    private FailureDetector failureDetector;
    private final Class<? extends FailureDetector> failureDetectorClass;
    private Store<String, String> store;
    private final VoldemortException exception;
    private Node node;

    public FailureDetectingStoreTest(Class<? extends FailureDetector> failureDetectorClass,
                                     VoldemortException exception) {
        this.failureDetectorClass = failureDetectorClass;
        this.exception = exception;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        node = new Node(0, "localhost", 1111, 2222, 3333, 0, Collections.singletonList(0));
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(100)
                                                                                 .setNodes(Collections.singletonList(node));
        failureDetector = create(failureDetectorConfig, false);
        store = AsyncUtils.asStore(new FailureDetectingStore<String, String>(node,
                                                                             failureDetector,
                                                                             AsyncUtils.asAsync(new FailingStore<String, String>("test",
                                                                                                                                 exception))));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

        if(failureDetector != null)
            failureDetector.destroy();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { BannagePeriodFailureDetector.class, new VoldemortException("Oops") },
                { BannagePeriodFailureDetector.class, new UnreachableStoreException("bad") } });
    }

    protected void checkException(Exception e) {
        assertEquals("Node is marked available",
                     !(e instanceof UnreachableStoreException),
                     failureDetector.isAvailable(node));
    }

    @Test
    public void testPut() {
        try {
            store.put("key", new Versioned<String>("value"));
            fail("Expected Failure");
        } catch(Exception e) {
            checkException(e);
        }
    }

    @Test
    public void testGet() {
        try {
            failureDetector.waitForAvailability(node);
            store.get("key");
            fail("Expected Failure");
        } catch(Exception e) {
            checkException(e);
        }
    }

    @Test
    public void testDelete() {
        try {
            failureDetector.waitForAvailability(node);
            store.delete("key", VersionFactory.newVersion());
            fail("Expected Failure");
        } catch(Exception e) {
            checkException(e);
        }
    }

    @Test
    public void testGetVersions() {
        try {
            failureDetector.waitForAvailability(node);
            store.getVersions("key");
            fail("Expected Failure");
        } catch(Exception e) {
            checkException(e);
        }
    }

    @Test
    public void testGetAll() {
        try {
            failureDetector.waitForAvailability(node);
            store.getAll(Collections.singletonList("key"));
            fail("Expected Failure");
        } catch(Exception e) {
            checkException(e);
        }
    }
}
