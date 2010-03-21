package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import voldemort.utils.ByteArray;
import voldemort.versioning.Version;

public class MysqlKeyedRow extends MysqlRow {

    private final ByteArray key;

    public MysqlKeyedRow(String table, Connection connection, String select, ByteArray key)
                                                                                           throws SQLException {
        super(table, connection);
        this.key = key;
        executeQuery(select);
    }

    public MysqlKeyedRow(String table, DataSource datasource, String select, ByteArray key)
                                                                                           throws SQLException {
        super(table, datasource);
        this.key = key;
        executeQuery(select);
    }

    private void executeQuery(String select) throws SQLException {
        statement = connection.prepareStatement(select);
        statement.setBytes(1, key.get());
        rs = statement.executeQuery();
    }

    @Override
    // TODO: This should not be necessary if select for update is setup
    // correctly
    protected void removeCurrent() throws SQLException {
        String sql = "delete from " + table + " where key_ = ? and version_ = ?";
        PreparedStatement deleteStmt = null;
        Version version = getVersion();
        deleteStmt = connection.prepareStatement(sql);
        deleteStmt.setBytes(1, key.get());
        deleteStmt.setBytes(2, version.toBytes());
        deleteStmt.executeUpdate();
    }
}
