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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class Timer {

    private String name;
    private long start;
    private long completed = 0;
    private long timeLimit;
    private List<Checkpoint> checkpoints;

    private final class Checkpoint {

        private final String point;
        private final long time;

        public Checkpoint(String point) {
            this.point = point;
            this.time = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return point + "@" + (time - start);
        }
    }

    public Timer(String name) {
        this(name, -1);
    }

    public Timer(String name, long limit) {
        this.name = name;
        this.timeLimit = limit;
        this.checkpoints = new ArrayList<Checkpoint>(5);
        reset();
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setExpiration(long limit) {
        this.timeLimit = limit;
    }

    public void reset() {
        reset(System.currentTimeMillis());
    }

    /**
     * Reset the start time to the input time, removing any checkpoints that
     * happened before this time.
     * 
     * @param startTime The new start time of the timer.
     */
    public synchronized void reset(long startTime) {
        this.start = startTime;
        this.completed = 0;
        int points = checkpoints.size();
        if(points > 0) { // If there are any checkpoints
            Checkpoint point = checkpoints.get(points - 1); // Get the newest
                                                            // (last) one
            if(point == null || point.time <= startTime) { // If it older than
                                                           // the start time
                checkpoints.clear(); // Delete them all
            } else { // Not newer
                // Delete the checkpoints up to the timer
                while(checkpoints.size() > 0 && checkpoints.get(0).time <= start) {
                    checkpoints.remove(0);
                }
            }
        }
    }

    public void checkpoint(Timer timer) {
        checkpoints.addAll(timer.checkpoints);
    }

    public void checkpoint(Checkpoint point) {
        checkpoints.add(point);
    }

    public boolean checkpoint(String point) {
        Checkpoint cp = new Checkpoint(point);
        checkpoints.add(cp);
        if(timeLimit > 0) {
            return (getDuration(cp.time) > timeLimit);
        } else {
            return false;
        }
    }

    public boolean expired() {
        long duration = getDuration();
        return duration > timeLimit;
    }

    public long getDuration() {
        if(completed > 0) {
            return getDuration(completed);
        } else {
            return getDuration(System.currentTimeMillis());
        }
    }

    public long getDuration(long current) {
        return current - start;
    }

    public long completed() {
        return completed(null);
    }

    public boolean isComplete() {
        return completed != 0;
    }

    public long completed(Logger logger) {
        return completed("", logger);
    }

    public long completed(String prefix, Logger logger) {
        if(completed == 0) {
            completed = System.currentTimeMillis();
            long duration = getDuration();
            if(duration > timeLimit) {
                if(logger != null) {
                    logger.warn(prefix + "Duration " + duration + " exceeded " + timeLimit + ": "
                                + toString());
                }
            }
            return duration;
        } else {
            return getDuration();
        }
    }

    @Override
    public String toString() {
        return "Timer " + name + ": duration=" + getDuration() + "; points=" + checkpoints;
    }
}
