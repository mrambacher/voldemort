package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import voldemort.store.PersistenceFailureException;

import com.google.common.collect.Lists;

public class MysqlUtils {

    private static final Log logger = LogFactory.getLog(MysqlUtils.class);

    public static void tryClose(ResultSet rs) {
        try {
            if(rs != null)
                rs.close();
        } catch(Exception e) {
            logger.error("Failed to close resultset.", e);
        }
    }

    public static void tryClose(Connection c) {
        try {
            if(c != null)
                c.close();
        } catch(Exception e) {
            logger.error("Failed to close connection.", e);
        }
    }

    public static void tryClose(PreparedStatement s) {
        try {
            if(s != null)
                s.close();
        } catch(Exception e) {
            logger.error("Failed to close prepared statement.", e);
        }
    }

    /**
     * Create Database
     */
    public static void createTableIfNotExists(Connection connection,
                                              String databaseName,
                                              String tableName,
                                              String tableSchema) {
        if(!tableExists(connection, databaseName, tableName)) {
            execute(connection, "create table " + databaseName + "." + tableName + tableSchema);
        }
    }

    private static boolean tableExists(Connection connection, String databaseName, String tableName) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "show tables in " + databaseName + " like '" + tableName + "'";
        try {
            stmt = connection.prepareStatement(select);
            rs = stmt.executeQuery();
            return rs.next();
        } catch(SQLException e) {
            throw new PersistenceFailureException("SQLException while checking for table existence in database "
                                                          + databaseName,
                                                  e);
        } finally {
            MysqlUtils.tryClose(rs);
            MysqlUtils.tryClose(stmt);
        }
    }

    /**
     * Create Database
     * 
     * @param connection The connection used for this operation.
     * @param databaseName Name of the database
     * @return true if it was created, false if it exists.
     */
    public static boolean createDatabaseIfNotExist(Connection connection, String databaseName) {
        if(!databaseExists(connection, databaseName)) {
            execute(connection, "create database " + databaseName + ";");
            return true;
        }
        return false;
    }

    /**
     * Utility method that executes a sql query
     * 
     * @param query The mysql query to execute.
     */
    public static void execute(DataSource datasource, String query) {
        Connection conn = null;
        try {
            conn = datasource.getConnection();
            execute(conn, query);
        } catch(SQLException e) {
            System.out.println("Caught SQL Exception " + e.getMessage());
            throw new PersistenceFailureException("SQLException while performing operation.", e);
        } finally {
            MysqlUtils.tryClose(conn);
        }
    }

    /**
     * Utility method that executes a sql query
     * 
     * @param query The mysql query to execute.
     */
    public static void execute(Connection conn, String query) {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(query);
            stmt.executeUpdate();
        } catch(SQLException e) {
            throw new PersistenceFailureException("SQLException while performing operation.", e);
        } finally {
            MysqlUtils.tryClose(stmt);
        }
    }

    private static boolean databaseExists(Connection connection, String databaseName) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "show databases like '" + databaseName + "'";
        try {
            stmt = connection.prepareStatement(select);
            rs = stmt.executeQuery();
            return rs.next();
        } catch(SQLException e) {
            throw new PersistenceFailureException("SQLException while checking for database existence!",
                                                  e);
        } finally {
            MysqlUtils.tryClose(rs);
            MysqlUtils.tryClose(stmt);
        }
    }

    public static List<String> getDatabasesWithPrefix(Connection connection, String databasePrefix) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<String> databaseNames = Lists.newArrayList();
        String select = "show databases like '" + databasePrefix + "%'";
        try {
            stmt = connection.prepareStatement(select);
            rs = stmt.executeQuery();
            while(rs.next()) {
                databaseNames.add(rs.getString(1));
            }
        } catch(SQLException e) {
            throw new PersistenceFailureException("SQLException while fetching database list!", e);
        } finally {
            MysqlUtils.tryClose(rs);
            MysqlUtils.tryClose(stmt);
        }
        return databaseNames;
    }

    public static void dropTable(Connection connection, String databaseName, String tableName) {
        if(tableExists(connection, databaseName, tableName)) {
            execute(connection, "drop table " + databaseName + "." + tableName);
        }
    }
}
