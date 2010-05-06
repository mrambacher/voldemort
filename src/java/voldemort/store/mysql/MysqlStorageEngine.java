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

package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import voldemort.store.AbstractStorageEngine;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreEntriesIterator;
import voldemort.store.StoreKeysIterator;
import voldemort.store.StoreRow;
import voldemort.store.StoreTransaction;
import voldemort.store.StoreVersionIterator;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseException;

/**
 * A StorageEngine that uses Mysql for persistence
 * 
 * 
 */
public class MysqlStorageEngine extends AbstractStorageEngine {

    private static final Logger logger = Logger.getLogger(MysqlStorageEngine.class);

    private final DataSource datasource;

    public MysqlStorageEngine(String name, DataSource datasource) {
        super(name);
        this.datasource = datasource;

        if(!tableExists()) {
            create();
        }
    }

    private boolean tableExists() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "show tables like '" + getName() + "'";
        try {
            conn = this.datasource.getConnection();
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            return rs.next();
        } catch(SQLException e) {
            System.err.println("Caught exception " + e);
            throw new PersistenceFailureException("SQLException while checking for table existence!",
                                                  e);
        } finally {
            MysqlUtils.tryClose(rs);
            MysqlUtils.tryClose(stmt);
            MysqlUtils.tryClose(conn);
        }
    }

    public void destroy() {
        execute("drop table if exists " + getName());
    }

    public void create() {
        MysqlUtils.execute(datasource,
                           "create table "
                                   + getName()
                                   + " (key_ varbinary(200) not null, version_ varbinary(200) not null, "
                                   + " value_ blob," + " metadata_ blob"
                                   + ", primary key(key_, version_)) engine = InnoDB");
    }

    public void execute(String query) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.executeUpdate();
        } catch(SQLException e) {
            System.out.println("Caught SQL Exception " + e.getMessage());
            throw new PersistenceFailureException("SQLException while performing operation.", e);
        } finally {
            MysqlUtils.tryClose(stmt);
            MysqlUtils.tryClose(conn);
        }
    }

    protected StoreRow getRowsForKey(ByteArray key, String sql) throws PersistenceFailureException {
        try {
            return new MysqlKeyedRow(getName(), this.datasource, sql, key);
        } catch(SQLException ex) {
            logger.error("Failed to create MySQLstore row - " + ex.getMessage(), ex);
            throw new PersistenceFailureException("Failed to create SQL store row", ex);
        }
    }

    @Override
    protected StoreRow getRowsForKey(ByteArray key) throws PersistenceFailureException {
        String select = "select version_, value_, metadata_ from " + getName() + " where key_ = ?";
        return getRowsForKey(key, select);
    }

    @Override
    protected ClosableIterator<Version> getVersionIterator(ByteArray key)
            throws PersistenceFailureException {
        String select = "select version_ from " + getName() + " where key_ = ?";
        StoreRow rows = getRowsForKey(key, select);
        return new StoreVersionIterator(rows);
    }

    public void truncate() {
        MysqlUtils.execute(datasource, "delete from " + getName());
    }

    @Override
    protected ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> getEntriesIterator() {
        String sql = "select key_, version_, value_, metadata_ from " + getName();
        try {
            StoreRow rows = new MysqlRow(getName(), this.datasource, sql);
            return new StoreEntriesIterator(rows);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        }
    }

    @Override
    protected ClosableIterator<ByteArray> getKeysIterator() {
        String sql = "select key_ from " + getName();
        try {
            StoreRow rows = new MysqlRow(getName(), this.datasource, sql);
            return new StoreKeysIterator(rows);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        }
    }

    @Override
    public StoreTransaction<Version> startTransaction(ByteArray key)
            throws PersistenceFailureException {
        try {
            return new MysqlTransaction(getName(), this.datasource, key);
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        }
    }

    public void close() throws PersistenceFailureException {
    // don't close datasource cause others could be using it
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }
}
