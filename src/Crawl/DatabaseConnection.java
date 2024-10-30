package Crawl;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnection {
    private static final String USER = "sa";
    private static final String PASSWORD = "123";
    private static final String CONTROL_DB_URL = "jdbc:sqlserver://DESKTOP-6PSGDFH\\SQLEXPRESS:1433;databaseName=Control;encrypt=false;trustServerCertificate=true;";
    private static final String STAGING_DB_URL = "jdbc:sqlserver://DESKTOP-6PSGDFH\\SQLEXPRESS:1433;databaseName=Staging;encrypt=false;trustServerCertificate=true;";

    private SQLServerDataSource dataSource;

    public DatabaseConnection() {
        dataSource = new SQLServerDataSource();
        dataSource.setUser(USER);
        dataSource.setPassword(PASSWORD);
    }

    public Connection getControlConnection() throws SQLException {
        dataSource.setURL(CONTROL_DB_URL);
        return dataSource.getConnection();
    }

    public Connection getStagingConnection() throws SQLException {
        dataSource.setURL(STAGING_DB_URL);
        return dataSource.getConnection();
    }

}
