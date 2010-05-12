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

import org.junit.Test;

import voldemort.VoldemortException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.utils.SystemTime;

public class ParallelTaskTest extends TestCase {

    private Callable<Integer> getCallable(final int id, final long ms, final Exception e) {
        return new Callable<Integer>() {

            public Integer call() throws Exception {
                try {
                    if(ms > 0)
                        Thread.sleep(ms);
                } catch(InterruptedException e) {

                }
                if(e != null) {
                    throw e;
                }
                return id;
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

    int getResults(Map<Integer, Callable<Integer>> callables,
                   int preferred,
                   int required,
                   long timeout) {
        ExecutorService pool = Executors.newFixedThreadPool(callables.size());
        long setup = System.currentTimeMillis();
        ParallelTask<Integer, Integer> tasks = ParallelTask.newInstance("test", pool, callables);
        // ParallelTask<Integer, Integer> tasks = new ParallelTask<Integer,
        // Integer>("test",
        // pool,
        // callables);
        long start = System.currentTimeMillis();
        Map<Integer, Integer> results = tasks.get(preferred,
                                                  required,
                                                  timeout,
                                                  TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - start;
        setup = System.currentTimeMillis() - setup;
        assertTrue(timeout + " ==" + elapsed, elapsed <= (timeout + 5));
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
            callables.put(i, getCallable(i, 5 + i * 50L, null));
        }
        try {
            assertEquals("Got all results", 2, getResults(callables, callables.size(), 2, 60));
        } catch(Exception e) {
            assertEquals("Unexpected exception",
                         InsufficientSuccessfulNodesException.class,
                         e.getClass());
        }
    }
}
