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
package voldemort.store.routed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.VoldemortException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.utils.SystemTime;

public class ParallelTaskTest extends TestCase {

    ExecutorService pool;
    ParallelTask<Integer, Integer> tasks;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if(tasks != null && !tasks.isDone()) {
            tasks.cancel(true);
        }
        if(pool != null) {
            pool.shutdown();
        }
        super.tearDown();
    }

    private Callable<Integer> getCallable(final int id, final long ms, final Exception e) {
        return new Callable<Integer>() {

            public Integer call() throws Exception {
                long start = System.currentTimeMillis();
                try {
                    try {
                        if(ms > 0)
                            Thread.sleep(ms);
                    } catch(InterruptedException e) {

                    }
                    if(e != null) {
                        throw e;
                    }
                    return id;
                } finally {
                    long current = System.currentTimeMillis();
                }
            }
        };
    }

    protected Map<Integer, Callable<Integer>> getCallables(int good,
                                                           int failed,
                                                           long ms,
                                                           Exception e) {
        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(9);
        for(int i = 0; i < good; i++) {
            callables.put(i, getCallable(i, ms, null));
        }
        for(int i = 0; i < failed; i++) {
            callables.put(i + good, getCallable(i + good, ms, e));
        }
        return callables;
    }

    void createThreadPool(int size) {
        pool = Executors.newFixedThreadPool(size);
    }

    ParallelTask<Integer, Integer> createTasks(String what,
                                               Map<Integer, Callable<Integer>> callables) {
        createThreadPool(callables.size());
        tasks = ParallelTask.newInstance(what, pool, callables);
        return tasks;
    }

    long getDuration() {
        return tasks.getTaskDuration(TimeUnit.MILLISECONDS);
    }

    long getDuration(long since) {
        return tasks.getTaskDuration(TimeUnit.MILLISECONDS) - since;
    }

    int getResults(Map<Integer, Callable<Integer>> callables,
                   int preferred,
                   int required,
                   long timeout) {
        createTasks("test", callables);
        return waitForResults(preferred, required, timeout);
    }

    int waitForResults(int preferred, int required, long timeout) {
        return waitForResults(preferred, required, timeout, timeout + timeout / 4);
    }

    int waitForResults(long timeout) {
        return waitForResults(timeout, timeout + timeout / 4);
    }

    int waitForResults(long timeout, long expectedTime) {
        long start = getDuration();
        Map<Integer, Integer> results = tasks.get(timeout, TimeUnit.MILLISECONDS);
        long elapsed = getDuration(start);
        if(expectedTime > 0) {
            assertTrue(timeout + " <=" + elapsed, elapsed <= expectedTime);
        }
        return results.size();
    }

    int waitForResults(int preferred, int required, long timeout, long expectedTime) {
        long start = getDuration();
        Map<Integer, Integer> results = tasks.get(preferred,
                                                  required,
                                                  timeout,
                                                  TimeUnit.MILLISECONDS);
        long elapsed = getDuration(start);
        if(expectedTime > 0) {
            assertTrue(timeout + " <=" + elapsed, elapsed <= expectedTime);
        }
        return results.size();
    }

    @Test
    public void testQueue() {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(10);
        long waitTime = 20 * SystemTime.NS_PER_MS;
        try {
            Integer result = queue.poll(waitTime, TimeUnit.NANOSECONDS);
            assertNull("No result expected", result);
        } catch(InterruptedException e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    @Test
    public void testBasic() {
        Map<Integer, Callable<Integer>> callables = getCallables(3, 0, 0, null);
        assertEquals("Got all results", callables.size(), getResults(callables,
                                                                     callables.size(),
                                                                     callables.size(),
                                                                     20));
    }

    @Test
    public void testSomeFailures() {
        Map<Integer, Callable<Integer>> callables = getCallables(3,
                                                                 0,
                                                                 0,
                                                                 new VoldemortException("Oops"));
        assertEquals("Got all results", 3, getResults(callables, callables.size(), 3, 100));
    }

    @Test
    public void testSomeSleepy() {
        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(9);
        for(int i = 0; i < 10; i++) {
            callables.put(i, getCallable(i, 5 + i * 50L, null));
        }
        assertEquals("Got all results", 2, getResults(callables, callables.size(), 2, 60));
    }

    @Test
    public void testTimeouts() {
        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(9);
        for(int i = 0; i < 10; i++) {
            callables.put(i, getCallable(i, 5 + i * 100L, null));
        }
        try {
            assertEquals("Got all results", 2, getResults(callables, callables.size(), 2, 125));
        } catch(Exception e) {
            assertEquals("Unexpected exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getClass());
        }
    }

    @Test
    public void testRequired() {
        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(3);
        callables.put(0, getCallable(0, 0L, null)); // One returns immediately
        callables.put(1, getCallable(1, 0L, null)); // One returns immediately
        // One returns next with an error
        callables.put(2, getCallable(2, 10L, new VoldemortException("Oops")));
        callables.put(3, getCallable(3, 100L, null)); // One that delays with
        // out an error

        try {
            assertEquals("Got all results", 3, getResults(callables, callables.size(), 3, 200));
        } catch(Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testRequiredWithEarlyFailures() {
        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(3);
        callables.put(0, getCallable(0, 0L, new VoldemortException("Oops")));
        callables.put(0, getCallable(0, 10L, null));
        callables.put(2, getCallable(2, 10L, null));
        callables.put(3, getCallable(3, 100L, null));

        try {
            assertEquals("Got all results", 3, getResults(callables, callables.size(), 3, 200));
        } catch(Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testPreferredWithTimeout() {
        // In this test, we have preferred=2; required=1. One returns
        // immediately with no error.
        // Another is returned later (after the timeout expires. Two other tasks
        // fail. No errors are expected
        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(3);
        callables.put(0, getCallable(0, 0L, null)); // One returns immediately
        callables.put(1, getCallable(1, 10L, new VoldemortException("Oops"))); // One
        // returns
        // next
        // with
        // an
        // error
        callables.put(2, getCallable(2, 10L, new VoldemortException("Oops"))); // One
        // returns
        // next
        // with
        // an
        // error
        callables.put(3, getCallable(3, 200L, null)); // One that delays with
        // out an error

        try {
            assertEquals("Got all results", 1, getResults(callables, 2, 1, 50));
        } catch(Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    // Ignoring the test
    // @Test
    // public void testRepeatedGets() {
    // // Create a bunch of callables that return at different intervals to
    // // test if we can do repeated gets appropriately
    //
    // Map<Integer, Callable<Integer>> callables = new HashMap<Integer,
    // Callable<Integer>>(6);
    // // One returns immediately
    // callables.put(0, getCallable(0, 0L, null));
    // // One returns immediately with an error
    // callables.put(1, getCallable(1, 0L, new VoldemortException("Oops")));
    // // One that delays with an error
    // callables.put(2, getCallable(2, 200L, new VoldemortException("Oops")));
    // // One that delays without an error
    // callables.put(3, getCallable(3, 200L, null));
    // // One that delays longer with an error
    // callables.put(4, getCallable(4, 400L, new VoldemortException("Oops")));
    // // One that delays longer without an error
    // callables.put(5, getCallable(5, 400L, null));
    //
    // this.createTasks("test", callables);
    // long start = getDuration();
    // // Wait 100 ms for the first batch
    // assertEquals("First result", 1, waitForResults(100));
    // // Wait another 200 ms for the next batch
    // assertEquals("Next result", 1, waitForResults(200 - getDuration(start)));
    // // Wait another 200 ms for the last batch
    // assertEquals("Last result", 1, waitForResults(400 - getDuration(start)));
    // // At this point, all results should have been collected. Try collecting
    // // more. Should return quickly
    // assertEquals("Returned no results", 0, waitForResults(500, 10));
    // }

    @Test
    public void testParallelCounters() {
        // Create a bunch of callables that return at different intervals to
        // test if we can do repeated gets appropriately

        Map<Integer, Callable<Integer>> callables = new HashMap<Integer, Callable<Integer>>(6);
        callables.put(0, getCallable(0, 0L, null)); // One returns immediately
        callables.put(1, getCallable(1, 1000L, null)); // 
        callables.put(2, getCallable(2, 2000L, null)); // One
        this.createTasks("test", callables);
        long start = getDuration();
        for(int i = 0; i <= 2; i++) {
            int results = waitForResults((100 + 1000 * i) - getDuration(start));
            assertEquals("Returned single result [" + i + "]", 1, results);
            assertEquals("Tasks retrieved [" + i + "]", i + 1, tasks.retrieved);
            assertEquals("Tasks still remaining [" + i + "]", 2 - i, tasks.getRemaining());
        }
    }
}
