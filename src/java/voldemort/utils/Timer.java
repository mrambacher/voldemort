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

    private final String name;
    private long start;
    private long completed = 0;
    private final long timeLimit;
    private List<Checkpoint> checkpoints;

    private final class Checkpoint {

        private final String point;
        private final long time;

        public Checkpoint(String point) {
            this.point = point;
            this.time = System.currentTimeMillis() - start;
        }

        @Override
        public String toString() {
            return point + "@" + time;
        }
    }

    public Timer(String name, long limit) {
        this.name = name;
        this.timeLimit = limit;
        this.checkpoints = new ArrayList<Checkpoint>(5);
        reset();
    }

    public void reset() {
        reset(System.currentTimeMillis());
    }

    public void reset(long startTime) {
        this.start = startTime;
        this.completed = 0;
        this.checkpoints.clear();
    }

    public boolean checkpoint(String point) {
        Checkpoint cp = new Checkpoint(point);
        checkpoints.add(cp);
        return expired(cp.time);
    }

    public boolean expired() {
        if(completed > 0) {
            return expired(completed);
        } else {
            return expired(System.currentTimeMillis());
        }
    }

    public boolean expired(long current) {
        long duration = getDuration(current);
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

    public long completed(Logger logger) {
        if(completed == 0) {
            completed = System.currentTimeMillis();
            long duration = getDuration();
            if(duration > timeLimit) {
                if(logger != null) {
                    logger.warn("Timer " + name + " exceeded " + timeLimit + "ms duration="
                                + getDuration() + "; points=" + checkpoints);
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
