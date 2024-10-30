package Crawl;

import java.sql.*;

public class DataService {
    private DatabaseConnection dbConnection;
    public DataService(DatabaseConnection dbConnection) {
        this.dbConnection = dbConnection;
    }
    public String[] getFileConfigData() {
        String[] result = new String[3];
        String sql = "SELECT id, source, source_file_location FROM config_file";

        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                result[0] = String.valueOf(rs.getInt("id"));
                result[1] = rs.getString("source");
                result[2] = rs.getString("source_file_location");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void updateFileLog(int idConfig, String filename, String status, int count, long filesize, String csvfile) {
        String sql = "INSERT INTO file_log (id_config, filename, time, status, count, filesize, dt_update, csvfile) " +
                "VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, CURRENT_TIMESTAMP, ?)";

        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, idConfig);
            pstmt.setString(2, filename);
            pstmt.setString(3, status);
            pstmt.setInt(4, count);
            pstmt.setLong(5, filesize);
            pstmt.setString(6, csvfile);

            pstmt.executeUpdate();
            System.out.println("Cập nhật bảng file_log thành công!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public String[] getFirstStatus() {
        String[] result = new String[4];
        String sql = "SELECT fl.status, cf.id AS id_config, fl.csvfile, cf.source_file_location " +
                "FROM config_file cf " +
                "JOIN file_log fl ON cf.id = fl.id_config ";
        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                result[0] = rs.getString("status");
                result[1] = String.valueOf(rs.getInt("id_config"));
                result[2] = rs.getString("csvfile");
                result[3] = rs.getString("source_file_location");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
    public boolean updateStatus(int configId, String newStatus) {
        String sql = "UPDATE file_log SET status = ? WHERE id_config = ?";
        boolean updated = false;
        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, newStatus);
            pstmt.setInt(2, configId);
            int rowsAffected = pstmt.executeUpdate();
            updated = rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return updated;
    }

    public void bulkInsertProducts(String csvFilePath) {
//        csvFilePath = csvFilePath.replace("\\\\", "\\");
        String sql = "BULK INSERT products " +
                "FROM '" + csvFilePath + "' " +
                "WITH ( " +
                "FIRSTROW = 2, " +
                "FIELDTERMINATOR = ',', " +
                "ROWTERMINATOR = '0x0a', " +
                "CODEPAGE = '65001', " +
                "MAXERRORS = 100 " +
                ");";

        try (Connection conn = dbConnection.getStagingConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Thêm thành công");
        } catch (SQLException e) {
            System.err.println("Thất bại: " + e.getMessage());
        }
    }


    public static void main(String[] args) {

        DataService dataService = new DataService(new DatabaseConnection());
        dataService.bulkInsertProducts("D:\\DataWH\\DrawlData\\products_20241030_203716.csv");

    }
}
