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

package voldemort.store.bdb;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreEntriesIterator;
import voldemort.store.StoreKeysIterator;
import voldemort.store.StoreRow;
import voldemort.store.StoreTransaction;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

/**
 * A store that uses BDB for persistence
 * 
 * @author jay
 * 
 */
public class BdbStorageEngine extends AbstractStorageEngine {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);

    private Database bdbDatabase;
    private final Environment environment;
    private final AtomicBoolean isOpen;
    private final boolean cursorPreload;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    public BdbStorageEngine(String name, Environment environment, Database database) {
        this(name, environment, database, false);
    }

    public BdbStorageEngine(String name,
                            Environment environment,
                            Database database,
                            boolean cursorPreload) {
        super(name);
        this.bdbDatabase = Utils.notNull(database);
        this.environment = Utils.notNull(environment);
        this.isOpen = new AtomicBoolean(true);
        this.cursorPreload = cursorPreload;
    }

    /**
     * Reopens the bdb Database after a successful truncate operation.
     */
    private boolean reopenBdbDatabase() {
        try {
            bdbDatabase = environment.openDatabase(null,
                                                   this.getName(),
                                                   this.bdbDatabase.getConfig());
            return true;
        } catch(DatabaseException e) {
            throw new StorageInitializationException("Failed to reinitialize BdbStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    /**
     * truncate() operation mandates that all opened Database be closed before
     * attempting truncation.
     * <p>
     * This method throws an exception while truncation is happening to any
     * request attempting in parallel with store truncation.
     * 
     * @return
     */
    private Database getBdbDatabase() {
        if(isTruncating.get()) {
            throw new VoldemortException("Bdb Store " + getName()
                                         + " is currently truncating cannot serve any request.");
        }

        return bdbDatabase;
    }

    public void truncate() {

        if(isTruncating.compareAndSet(false, true)) {
            Transaction transaction = environment.beginTransaction(null, null);
            boolean succeeded = false;

            try {
                // close current bdbDatabase first
                bdbDatabase.close();

                // truncate the database
                environment.truncateDatabase(transaction, this.getName(), false);
                succeeded = true;
            } catch(DatabaseException e) {
                logger.error(e);
                throw new VoldemortException("Failed to truncate Bdb store " + getName(), e);

            } finally {
                BdbTransaction.commitOrAbort(transaction, succeeded);

                // reopen the bdb database for future queries.
                if(reopenBdbDatabase()) {
                    isTruncating.compareAndSet(true, false);
                } else {
                    throw new VoldemortException("Failed to reopen Bdb Database after truncation, All request will fail on store "
                                                 + getName());
                }
            }
        } else {
            throw new VoldemortException("Store " + getName()
                                         + " is already truncating, cannot start another one.");
        }
    }

    @Override
    protected StoreRow getRowsForKey(ByteArray key) throws PersistenceFailureException {
        try {
            Database db = this.getBdbDatabase();
            StoreRow rows = new BdbStoreKeyedRow(key, db, LockMode.READ_UNCOMMITTED);
            return rows;
        } catch(DatabaseException ex) {
            logger.error("Failed to create BDB store row", ex);
            throw new PersistenceFailureException("Failed to create BDB store row", ex);
        }
    }

    @Override
    protected ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> getEntriesIterator() {
        try {
            Database db = getBdbDatabase();
            if(cursorPreload) {
                PreloadConfig preloadConfig = new PreloadConfig();
                preloadConfig.setLoadLNs(true);
                db.preload(preloadConfig);
            }
            StoreRow rows = new BdbStoreRow(db, LockMode.READ_UNCOMMITTED);
            return new StoreEntriesIterator(rows);
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    @Override
    protected ClosableIterator<ByteArray> getKeysIterator() {
        try {
            Database db = getBdbDatabase();
            if(cursorPreload) {
                PreloadConfig preloadConfig = new PreloadConfig();
                preloadConfig.setLoadLNs(true);
                db.preload(preloadConfig);
            }
            StoreRow rows = new BdbStoreRow(db, LockMode.READ_UNCOMMITTED);
            return new StoreKeysIterator(rows);
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    @Override
    public StoreTransaction<Version> startTransaction(ByteArray key)
            throws PersistenceFailureException {
        try {
            Database db = getBdbDatabase();
            return new BdbTransaction(environment, db, key);
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public void close() throws PersistenceFailureException {
        try {
            if(this.isOpen.compareAndSet(true, false)) {
                this.getBdbDatabase().close();
            }
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    public DatabaseStats getStats(boolean setFast) {
        try {
            StatsConfig config = new StatsConfig();
            config.setFast(setFast);
            Database db = getBdbDatabase();
            return db.getStats(config);
        } catch(DatabaseException e) {
            logger.error(e);
            throw new VoldemortException(e);
        }
    }

    @JmxOperation(description = "A variety of stats about the BDB for this store.")
    public String getBdbStats() {
        String stats = getStats(true).toString();
        return stats;
    }
}
