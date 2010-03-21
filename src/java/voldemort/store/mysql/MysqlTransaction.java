package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreRow;
import voldemort.store.StoreTransaction;
import voldemort.store.StoreVersionIterator;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * @author abbagri
 */
public class MysqlTransaction implements StoreTransaction<Version> {

    private static int MYSQL_ERR_DUP_KEY = 1022;

    private static int MYSQL_ERR_DUP_ENTRY = 1062;

    private ByteArray key;
    private Connection connection;
    private String storeName;

    public MysqlTransaction(String storeName, DataSource datasource, ByteArray key) {
        this.storeName = storeName;
        this.key = key;
        try {
            connection = datasource.getConnection();
            connection.setAutoCommit(false);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Error when trying to get connection from datasource "
                                                          + key.toString() + e.getMessage(),
                                                  e);
        }
    }

    public ClosableIterator<Version> getIterator() throws PersistenceFailureException {
        try {
            String sql = "select version_ from " + storeName + " where key_ = ? for update";
            StoreRow rows = new MysqlKeyedRow(storeName, this.connection, sql, key);
            return new StoreVersionIterator(rows);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Error selecting row - " + e.getMessage(), e);
        }
    }

    public void update(ClosableIterator<Version> iter, Versioned<byte[]> value) {
        iter.remove();
        insert(iter, value);
    }

    public void insert(ClosableIterator<Version> iter, Versioned<byte[]> value)
            throws PersistenceFailureException {
        String insertSql = "insert into " + storeName
                           + " (key_, version_, value_, metadata_) values (?, ?, ?, ?)";
        PreparedStatement insert = null;
        try {
            insert = connection.prepareStatement(insertSql);
            insert.setBytes(1, key.get());
            insert.setBytes(2, value.getVersion().toBytes());
            insert.setBytes(3, value.getValue());
            insert.setBytes(4, value.getMetadata().toBytes());
            insert.executeUpdate();
        } catch(SQLException e) {
            if(e.getErrorCode() == MYSQL_ERR_DUP_KEY || e.getErrorCode() == MYSQL_ERR_DUP_ENTRY) {
                throw new ObsoleteVersionException("Key or value already used.");
            } else {
                throw new PersistenceFailureException("Error when trying to insert key "
                                                      + key.toString() + e.getMessage(), e);
            }
        } finally {
            MysqlUtils.tryClose(insert);
        }
    }

    public void close(boolean commit) throws PersistenceFailureException {
        try {
            if(connection != null) {
                if(commit)
                    connection.commit();
                else
                    connection.rollback();
            }
        } catch(SQLException e) {
            throw new PersistenceFailureException("Error when trying to commit transaction: "
                                                  + e.getMessage(), e);
        } finally {
            MysqlUtils.tryClose(connection);
        }
    }
}