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
package voldemort.utils;

import junit.framework.TestCase;

import org.junit.Test;

public class TimerTest extends TestCase {

    private void delay(int ms) {
        try {
            Thread.sleep(ms);
        } catch(InterruptedException e) {

        }
    }

    @Test
    public void testExpired() {
        Timer timer = new Timer("test", 150);
        delay(100);
        assertFalse("Checkpoint valid", timer.expired());
        delay(100);
        timer.completed();
        assertTrue("Timer expired", timer.expired());
    }

    @Test
    public void testCheckpoint() {
        Timer timer = new Timer("test", 100);
        for(int i = 1; i < 5; i++) {
            delay(10);
            assertFalse("Checkpoint valid for " + i, timer.checkpoint(Integer.toString(i)));
        }
        delay(100);
        assertTrue("Timer expired", timer.expired());
        long duration = timer.completed();

    }

}
