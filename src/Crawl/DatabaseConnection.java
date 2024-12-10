package Crawl;

import com.google.gson.JsonObject;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnection {

    private SQLServerDataSource controlDataSource;
    private SQLServerDataSource stagingDataSource;
    private SQLServerDataSource dwDataSource;

    public DatabaseConnection(JsonObject config) {
        try {
            JsonObject controlDbConfig = config.getAsJsonObject("databases").getAsJsonObject("control");
            controlDataSource = createDataSource(controlDbConfig);

            JsonObject stagingDbConfig = config.getAsJsonObject("databases").getAsJsonObject("staging");
            stagingDataSource = createDataSource(stagingDbConfig);

            JsonObject dwDbConfig = config.getAsJsonObject("databases").getAsJsonObject("dw");
            dwDataSource = createDataSource(dwDbConfig);
        } catch (Exception e) {
            System.err.println("Lỗi khi đọc cấu hình kết nối: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private SQLServerDataSource createDataSource(JsonObject dbConfig) {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(dbConfig.get("url").getAsString());
        dataSource.setUser(dbConfig.get("user").getAsString());
        dataSource.setPassword(dbConfig.get("password").getAsString());
        return dataSource;
    }

    public Connection getControlConnection() {
        return getConnection(controlDataSource);
    }

    public Connection getStagingConnection() {
        return getConnection(stagingDataSource);
    }

    public Connection getDWConnection() {
        return getConnection(dwDataSource);
    }

    private Connection getConnection(SQLServerDataSource dataSource) {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            System.err.println("Không kết nối được database: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

}










//
//
//private static final String USER = "sa";
//private static final String PASSWORD = "123";
//private static final String CONTROL_DB_URL = "jdbc:sqlserver://DESKTOP-6PSGDFH\\SQLEXPRESS:1433;databaseName=Control;encrypt=false;trustServerCertificate=true;";
//private static final String STAGING_DB_URL = "jdbc:sqlserver://DESKTOP-6PSGDFH\\SQLEXPRESS:1433;databaseName=Staging;encrypt=false;trustServerCertificate=true;";
//private static final String DW_DB_URL = "jdbc:sqlserver://DESKTOP-6PSGDFH\\SQLEXPRESS:1433;databaseName=DW;encrypt=false;trustServerCertificate=true;";
//private SQLServerDataSource dataSource;
//
//public DatabaseConnection() {
//    dataSource = new SQLServerDataSource();
//    dataSource.setUser(USER);
//    dataSource.setPassword(PASSWORD);
//}
//
//public Connection getControlConnection() {
//    return getConnection(CONTROL_DB_URL);
//}
//
//public Connection getStagingConnection() {
//    return getConnection(STAGING_DB_URL);
//}
//
//public Connection getDWConnection() {
//    return getConnection(DW_DB_URL);
//}
//
//private Connection getConnection(String dbUrl) {
//    dataSource.setURL(dbUrl);
//    try {
//        return dataSource.getConnection();
//    } catch (SQLException e) {
//        System.err.println("không kết nối được database " + e.getMessage());
//        e.printStackTrace();
//        return null;
//    }
//}
