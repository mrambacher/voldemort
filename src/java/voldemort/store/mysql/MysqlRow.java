package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreRow;
import voldemort.utils.ByteArray;
import voldemort.versioning.Metadata;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

public class MysqlRow implements StoreRow {

    /**
     * The result set which serves the next() calls after having made the
     * request in the first call
     */
    protected ResultSet rs;
    private boolean advanced = false;
    private boolean first = true;
    private boolean hasMore = false;
    protected Connection connection = null;
    protected PreparedStatement statement = null;
    private final boolean closeConnection;
    protected final String table;

    public MysqlRow(String table, Connection connection) {
        this.table = table;
        this.connection = connection;
        this.closeConnection = false;
    }

    public MysqlRow(String table, DataSource datasource) throws SQLException {
        this.table = table;
        this.connection = datasource.getConnection();
        this.closeConnection = true;
    }

    public MysqlRow(String table, DataSource datasource, String select) throws SQLException {
        this(table, datasource);
        statement = connection.prepareStatement(select);
        rs = statement.executeQuery();
    }

    public Versioned<byte[]> getVersioned() {
        try {
            Version version = getVersion();
            Metadata metadata = getMetadata();
            byte[] bytes = rs.getBytes("value_");
            return new Versioned<byte[]>(bytes, version, metadata);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me! - " + e.getMessage(), e);
        }
    }

    public Version getVersion() {
        try {
            byte[] bytes = rs.getBytes("version_");
            return VersionFactory.toVersion(bytes);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me! - " + e.getMessage(), e);
        }
    }

    public Metadata getMetadata() {
        try {
            byte[] bytes = rs.getBytes("metadata_");
            return VersionFactory.toMetadata(bytes);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me! - " + e.getMessage(), e);
        }
    }

    public ByteArray getKey() {
        try {
            byte[] bytes = rs.getBytes("key_");
            return new ByteArray(bytes);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        }
    }

    public void close() {
        MysqlUtils.tryClose(rs);
        MysqlUtils.tryClose(statement);
        if(this.closeConnection) {
            MysqlUtils.tryClose(connection);
        }
    }

    protected void removeCurrent() throws SQLException {
        if(rs != null) {
            rs.deleteRow();
        }
    }

    public void remove() {
        if(rs == null) {
            throw new PersistenceFailureException("Call to remove() on a closed iterator.");
        } else if(advanced) {
            throw new PersistenceFailureException("Call to remove() on an advanced iterator.");
        } else {
            try {
                removeCurrent();
            } catch(SQLException e) {
                throw new PersistenceFailureException("Failed to delete row - " + e.getMessage(), e);
            }
        }
    }

    public boolean advance() {
        try {
            if(advanced) {
                advanced = false;
                return true;
            } else if(first) {
                first = false;
                return rs.first();
            } else {
                return rs.next();
            }
        } catch(SQLException e) {
            throw new PersistenceFailureException("Failed to advance cursor - " + e.getMessage(), e);
        }
    }

    public boolean hasNext() {
        if(rs == null) {
            return false;
        } else if(!advanced) {
            hasMore = this.advance();
            advanced = hasMore;
        }
        return hasMore;
    }
}
