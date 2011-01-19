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

package voldemort.server.scheduler;

import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.client.protocol.VoldemortFilter;
import voldemort.store.StorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Expire old data
 * 
 * 
 */
public class DataCleanupJob<K, V, T> implements Runnable {

    private static final Logger logger = Logger.getLogger(DataCleanupJob.class);

    private final StorageEngine<K, V, T> store;
    private final Semaphore cleanupPermits;
    private final long maxAgeMs;
    private final Time time;
    private final EventThrottler throttler;

    public DataCleanupJob(StorageEngine<K, V, T> store,
                          Semaphore cleanupPermits,
                          long maxAgeMs,
                          Time time,
                          EventThrottler throttler) {
        this.store = Utils.notNull(store);
        this.cleanupPermits = Utils.notNull(cleanupPermits);
        this.maxAgeMs = maxAgeMs;
        this.time = time;
        this.throttler = throttler;
    }

    public void run() {
        acquireCleanupPermit();
        ClosableIterator<Pair<K, Versioned<V>>> iterator = null;
        try {
            logger.info("Starting data cleanup on store \"" + store.getName() + "\"...");
            int deleted = 0;
            final long now = time.getMilliseconds();
            VoldemortFilter filter = new VoldemortFilter() {

                public boolean accept(Object key, Versioned<?> value) {
                    Version clock = value.getVersion();
                    return (now - clock.getTimestamp() > maxAgeMs);
                }
            };
            iterator = store.entries(filter);

            while(iterator.hasNext()) {
                // check if we have been interrupted
                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Datacleanup job halted.");
                    return;
                }

                Pair<K, Versioned<V>> keyAndVal = iterator.next();
                Version clock = keyAndVal.getSecond().getVersion();
                store.delete(keyAndVal.getFirst(), clock);
                deleted++;
                if(deleted % 10000 == 0)
                    logger.debug("Deleted item " + deleted);

                // throttle on number of entries.
                throttler.maybeThrottle(1);
            }
            logger.info("Data cleanup on store \"" + store.getName() + "\" is complete; " + deleted
                        + " items deleted.");
        } catch(Exception e) {
            logger.error("Error in data cleanup job for store " + store.getName() + ": ", e);
        } finally {
            closeIterator(iterator);
            logger.info("Releasing lock  after data cleanup on \"" + store.getName() + "\".");
            this.cleanupPermits.release();
        }
    }

    private void closeIterator(ClosableIterator<Pair<K, Versioned<V>>> iterator) {
        try {
            if(iterator != null)
                iterator.close();
        } catch(Exception e) {
            logger.error("Error in closing iterator " + store.getName() + " ", e);
        }
    }

    private void acquireCleanupPermit() {
        logger.info("Acquiring lock to perform data cleanup on \"" + store.getName() + "\".");
        try {
            this.cleanupPermits.acquire();
        } catch(InterruptedException e) {
            throw new IllegalStateException("Datacleanup interrupted while waiting for cleanup permit.",
                                            e);
        }
    }

}
