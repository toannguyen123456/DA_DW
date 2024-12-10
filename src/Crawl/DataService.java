package Crawl;

import model.TempExistsRecord;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class DataService {
    private DatabaseConnection dbConnection;

    public DataService(DatabaseConnection dbConnection) {
        this.dbConnection = dbConnection;
    }

    public String[] getFileConfigData(int sourceid) {
        String[] result = new String[3];
        String sql = "SELECT id, source, source_file_location FROM config_file WHERE id = ?";

        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, sourceid);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    result[0] = String.valueOf(rs.getInt("id"));
                    result[1] = rs.getString("source");
                    result[2] = rs.getString("source_file_location");
                } else {
                    System.out.println("No data found for sourceid: " + sourceid);
                }
            }
        } catch (SQLException e) {
            System.err.println("Database operation failed: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }


    public void updateFileLog(int idConfig, String filename, String status, int count, long filesize) {
        // Modify SQL to use CONVERT to format the date to 'yyyy-MM-dd' without time
        String sql = "INSERT INTO file_log (id_config, filename, time, status, count, filesize, dt_update) " +
                "VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, CONVERT(VARCHAR(10), GETDATE(), 120))";

        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // Bind the parameters in the correct order
            pstmt.setInt(1, idConfig);        // Bind idConfig
            pstmt.setString(2, filename);     // Bind filename
            pstmt.setString(3, status);       // Bind status
            pstmt.setInt(4, count);           // Bind count
            pstmt.setLong(5, filesize);       // Bind filesize

            // Execute the update
            pstmt.executeUpdate();
            System.out.println("Cập nhật bảng file_log thành công!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean updateAllStatusTRtoSC() {
        String sql = "UPDATE file_log SET status = 'SC' WHERE status = 'TR'";
        boolean updated = false;
        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int rowsAffected = pstmt.executeUpdate();
            updated = rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return updated;
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

    public String getStatus(String configId) {
        String sql = "SELECT status FROM file_log WHERE status = ?";
        String status = null;

        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, configId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    status = rs.getString("status");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return status;
    }


    public String[] getFirstStatus(String formattedDate) {
        String[] result = new String[4];
        String sql = "SELECT fl.status, cf.id AS id_config, fl.filename, cf.source_file_location " +
                "FROM config_file cf " +
                "JOIN file_log fl ON cf.id = fl.id_config " +
                "WHERE fl.status = ? OR CAST(fl.dt_update AS DATE) = ?";

        try (Connection conn = dbConnection.getControlConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, "ER");
            pstmt.setString(2, formattedDate);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    result[0] = rs.getString("status");
                    result[1] = String.valueOf(rs.getInt("id_config"));
                    result[2] = rs.getString("filename");
                    result[3] = rs.getString("source_file_location");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }


    public void bulkInsertProducts(String csvFilePath) {
        String sql = "BULK INSERT products_temps_1 " +
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
            System.out.println("aloalo");
            stmt.execute(sql);
            System.out.println("Thêm thành công");
        } catch (SQLException e) {
            System.err.println("thêm dữ liệu thất bại " + e.getMessage());
        }
    }


    public boolean cleanData() {
        String cleanSQL = "UPDATE products_temps_1 SET " +
                "productId = REPLACE(productId, '\"', ''), " +
                "localDate = REPLACE(localDate, '\"', ''), " +
                "imageURL = REPLACE(imageURL, '\"', ''), " +
                "productURL = REPLACE(productURL, '\"', ''), " +
                "productTitle = REPLACE(productTitle, '\"', ''), " +
                "price = REPLACE(price, '\"', ''), " +
                "discount_percentage = REPLACE(discount_percentage, '\"', ''), " +
                "brand = REPLACE(brand, '\"', ''), " +
                "material = REPLACE(material, '\"', ''), " +
                "style = REPLACE(style, '\"', ''), " +
                "warranty = REPLACE(warranty, '\"', ''), " +
                "colors = REPLACE(colors, '\"', '')";

        try (Connection conn = dbConnection.getStagingConnection();
             PreparedStatement cleanStmt = conn.prepareStatement(cleanSQL)) {
            int rowsUpdated = cleanStmt.executeUpdate();
            conn.commit();
            System.out.println("Rows updated: " + rowsUpdated);
            System.out.println("Thành công");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    public boolean cleanAndTransferData() {
        // Gọi hàm cleanData để làm sạch dữ liệu
        if (!cleanData()) {
            System.out.println("Lỗi khi làm sạch dữ liệu trong bảng products_temps_1.");
            return false;
        }

        String selectSQL = "SELECT productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors FROM products_temps_1";
        String insertSQL = "INSERT INTO products_daily_1 (localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dbConnection.getStagingConnection();
             PreparedStatement selectStmt = conn.prepareStatement(selectSQL);
             ResultSet rs = selectStmt.executeQuery()) {

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

            while (rs.next()) {
//                String productIdStr = rs.getString("productId");
                String localDateStr = rs.getString("localDate");
                String imageURL = rs.getString("imageURL");
                String productURL = rs.getString("productURL");
                String productTitle = rs.getString("productTitle");
                String priceStr = rs.getString("price");
                float price = 0.0f;
                if (priceStr != null && !priceStr.isEmpty()) {
                    price = Float.parseFloat(priceStr);
                }
                String discountPercentageStr = rs.getString("discount_percentage");
                int discountPercentage = 0;
                if (discountPercentageStr != null && !discountPercentageStr.isEmpty()) {
                    discountPercentage = Integer.parseInt(discountPercentageStr);
                }

                String brand = rs.getString("brand");
                String material = rs.getString("material");
                String style = rs.getString("style");
                String warranty = rs.getString("warranty");
                String colors = rs.getString("colors");

//                int productId = Integer.parseInt(productIdStr);

                LocalDate localDate = LocalDate.parse(localDateStr, formatter);
                System.out.println("Parsed localDate: " + localDate);
                // Query date_dim to get _1 value for the matching date
                String dateDimSQL = "SELECT _1 FROM date_dim WHERE _2005_01_01 = ?";
                try (PreparedStatement dateDimStmt = conn.prepareStatement(dateDimSQL)) {
                    dateDimStmt.setString(1, localDateStr);
                    ResultSet dateDimRS = dateDimStmt.executeQuery();

                    int dateDimId = 0;
                    if (dateDimRS.next()) {
                        dateDimId = dateDimRS.getInt("_1");
                    }

                    // Insert data into products_daily_1 with the retrieved date_dim_id
                    try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
//                        insertStmt.setInt(1, productId);
                        insertStmt.setDate(1, Date.valueOf
                                (localDate));
                        insertStmt.setString(2, imageURL);
                        insertStmt.setString(3, productURL);
                        insertStmt.setString(4, productTitle);
                        insertStmt.setFloat(5, price);
                        insertStmt.setInt(6, discountPercentage);
                        insertStmt.setString(7, brand);
                        insertStmt.setString(8, material);
                        insertStmt.setString(9, style);
                        insertStmt.setString(10, warranty);
                        insertStmt.setString(11, colors);
                        insertStmt.setInt(12, dateDimId);
                        insertStmt.executeUpdate();
                    }
                }
            }
            return true;

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NumberFormatException | DateTimeParseException e) {
            System.out.println("Lỗi khi chuyển đổi dữ liệu: " + e.getMessage());
        }
        return false;
    }

    public void deleteAllProducts() {
        String sql = "DELETE FROM products_temps_1;";

        try (Connection conn = dbConnection.getStagingConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Xóa dữ liệu thành công");
        } catch (SQLException e) {
            System.err.println("Thất bại: " + e.getMessage());
        }
    }

    public boolean checkAndInsertData() {
        String selectSQL = "SELECT productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id FROM products_daily_1";
        String selectDWSQL = "SELECT * FROM gong_kinh WHERE productId = ?";
        String insertTempExistsSQL = "INSERT INTO temp_exists (productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String insertTempNotExistsSQL = "INSERT INTO temp_not_exists (productId,localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
        String insertTempsChangeSQL = "INSERT INTO temp_exists_change (productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection stagingConn = dbConnection.getStagingConnection();
             PreparedStatement selectStmt = stagingConn.prepareStatement(selectSQL);
             ResultSet rs = selectStmt.executeQuery()) {

            // Create a new connection for the DW database
            try (Connection dwConn = dbConnection.getDWConnection()) {
                while (rs.next()) {
                    int productId = rs.getInt("productId");
                    String localDateStr = rs.getString("localDate");
                    String imageURL = rs.getString("imageURL");
                    String productURL = rs.getString("productURL");
                    String productTitle = rs.getString("productTitle");
                    float price = rs.getFloat("price");
                    int discountPercentage = rs.getInt("discount_percentage");
                    String brand = rs.getString("brand");
                    String material = rs.getString("material");
                    String style = rs.getString("style");
                    String warranty = rs.getString("warranty");
                    String colors = rs.getString("colors");
                    int dateDimId = rs.getInt("date_dim_id");

                    LocalDate localDate = LocalDate.parse(localDateStr);

                    // Kiểm tra xem dữ liệu này đã tồn tại trong DW chưa
                    try (PreparedStatement selectDWStmt = dwConn.prepareStatement(selectDWSQL)) {
                        selectDWStmt.setInt(1, productId);

                        ResultSet dwRS = selectDWStmt.executeQuery();

                        if (!dwRS.next()) {
                            // Dữ liệu không tồn tại trong DW, insert vào bảng temp_exists
                            try (PreparedStatement insertTempNotExistsStmt = dwConn.prepareStatement(insertTempExistsSQL)) {
                                insertTempNotExistsStmt.setInt(1, productId);
                                insertTempNotExistsStmt.setDate(2, Date.valueOf(localDate));
                                insertTempNotExistsStmt.setString(3, imageURL);
                                insertTempNotExistsStmt.setString(4, productURL);
                                insertTempNotExistsStmt.setString(5, productTitle);
                                insertTempNotExistsStmt.setFloat(6, price);
                                insertTempNotExistsStmt.setInt(7, discountPercentage);
                                insertTempNotExistsStmt.setString(8, brand);
                                insertTempNotExistsStmt.setString(9, material);
                                insertTempNotExistsStmt.setString(10, style);
                                insertTempNotExistsStmt.setString(11, warranty);
                                insertTempNotExistsStmt.setString(12, colors);
                                insertTempNotExistsStmt.setInt(13, dateDimId);
                                insertTempNotExistsStmt.executeUpdate();
                            }
                        } else {
                            // Dữ liệu tồn tại trong DW, kiểm tra sự thay đổi
                            String dwImageURL = dwRS.getString("imageURL");
                            String dwProductURL = dwRS.getString("productURL");
                            String dwProductTitle = dwRS.getString("productTitle");
                            float dwPrice = dwRS.getFloat("price");
                            int dwDiscountPercentage = dwRS.getInt("discount_percentage");
                            String dwBrand = dwRS.getString("brand");
                            String dwMaterial = dwRS.getString("material");
                            String dwStyle = dwRS.getString("style");
                            String dwWarranty = dwRS.getString("warranty");
                            String dwColors = dwRS.getString("colors");
                            int dwDateDimId = dwRS.getInt("date_dim_id");

                            // Kiểm tra xem có sự thay đổi dữ liệu không
                            if (!imageURL.equals(dwImageURL) || !productURL.equals(dwProductURL) || !productTitle.equals(dwProductTitle) ||
                                    price != dwPrice || discountPercentage != dwDiscountPercentage || !brand.equals(dwBrand) ||
                                    !material.equals(dwMaterial) || !style.equals(dwStyle) || !warranty.equals(dwWarranty) ||
                                    !colors.equals(dwColors) || dateDimId != dwDateDimId) {

                                // Nếu có sự thay đổi, insert vào bảng temp_exists_change
                                try (PreparedStatement insertTempsChangeStmt = dwConn.prepareStatement(insertTempsChangeSQL)) {
                                    insertTempsChangeStmt.setInt(1, productId);
                                    insertTempsChangeStmt.setDate(2, Date.valueOf(localDate));
                                    insertTempsChangeStmt.setString(3, imageURL);
                                    insertTempsChangeStmt.setString(4, productURL);
                                    insertTempsChangeStmt.setString(5, productTitle);
                                    insertTempsChangeStmt.setFloat(6, price);
                                    insertTempsChangeStmt.setInt(7, discountPercentage);
                                    insertTempsChangeStmt.setString(8, brand);
                                    insertTempsChangeStmt.setString(9, material);
                                    insertTempsChangeStmt.setString(10, style);
                                    insertTempsChangeStmt.setString(11, warranty);
                                    insertTempsChangeStmt.setString(12, colors);
                                    insertTempsChangeStmt.setInt(13, dateDimId);
                                    insertTempsChangeStmt.executeUpdate();
                                }
                            } else {
                                // Nếu không có sự thay đổi, insert vào bảng temp_not_exists
                                try (PreparedStatement insertTempExistsStmt = dwConn.prepareStatement(insertTempNotExistsSQL)) {
                                    insertTempExistsStmt.setInt(1, productId);
                                    insertTempExistsStmt.setDate(2, Date.valueOf(localDate));
                                    insertTempExistsStmt.setString(3, imageURL);
                                    insertTempExistsStmt.setString(4, productURL);
                                    insertTempExistsStmt.setString(5, productTitle);
                                    insertTempExistsStmt.setFloat(6, price);
                                    insertTempExistsStmt.setInt(7, discountPercentage);
                                    insertTempExistsStmt.setString(8, brand);
                                    insertTempExistsStmt.setString(9, material);
                                    insertTempExistsStmt.setString(10, style);
                                    insertTempExistsStmt.setString(11, warranty);
                                    insertTempExistsStmt.setString(12, colors);
                                    insertTempExistsStmt.setInt(13, dateDimId);
                                    insertTempExistsStmt.executeUpdate();
                                }
                            }
                        }
                    }
                }
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        } catch (SQLException | DateTimeParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<TempExistsRecord> getAllTempExistsData() {
        String selectSQL = "SELECT productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id FROM temp_exists";
        List<TempExistsRecord> tempExistsData = new ArrayList<>();

        try (Connection conn = dbConnection.getDWConnection();
             PreparedStatement selectStmt = conn.prepareStatement(selectSQL);
             ResultSet rs = selectStmt.executeQuery()) {

            while (rs.next()) {
                int productId = rs.getInt("productId");
                Date localDate = rs.getDate("localDate");
                String imageURL = rs.getString("imageURL");
                String productURL = rs.getString("productURL");
                String productTitle = rs.getString("productTitle");
                float price = rs.getFloat("price");
                int discountPercentage = rs.getInt("discount_percentage");
                String brand = rs.getString("brand");
                String material = rs.getString("material");
                String style = rs.getString("style");
                String warranty = rs.getString("warranty");
                String colors = rs.getString("colors");
                int dateDimId = rs.getInt("date_dim_id");

                // Create a record object to hold the retrieved data
                TempExistsRecord record = new TempExistsRecord(productId, localDate, imageURL, productURL, productTitle, price, discountPercentage, brand, material, style, warranty, colors, dateDimId);
                tempExistsData.add(record);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tempExistsData;
    }


    public List<TempExistsRecord> getAllTempExistsChange() {
        String selectSQL = "SELECT productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id FROM temp_exists_change";
        List<TempExistsRecord> tempExistsData = new ArrayList<>();

        try (Connection conn = dbConnection.getDWConnection();
             PreparedStatement selectStmt = conn.prepareStatement(selectSQL);
             ResultSet rs = selectStmt.executeQuery()) {

            while (rs.next()) {
                int productId = rs.getInt("productId");
                Date localDate = rs.getDate("localDate");
                String imageURL = rs.getString("imageURL");
                String productURL = rs.getString("productURL");
                String productTitle = rs.getString("productTitle");
                float price = rs.getFloat("price");
                int discountPercentage = rs.getInt("discount_percentage");
                String brand = rs.getString("brand");
                String material = rs.getString("material");
                String style = rs.getString("style");
                String warranty = rs.getString("warranty");
                String colors = rs.getString("colors");
                int dateDimId = rs.getInt("date_dim_id");

                // Create a record object to hold the retrieved data
                TempExistsRecord record = new TempExistsRecord(productId, localDate, imageURL, productURL, productTitle, price, discountPercentage, brand, material, style, warranty, colors, dateDimId);
                tempExistsData.add(record);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tempExistsData;
    }


    public boolean syncTempExistsChangeToGongKinh() {
        String selectTempExistsChangeSQL = "SELECT productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id FROM temp_exists_change";
        String checkGongKinhSQL = "SELECT * FROM gong_kinh WHERE productId = ?";
        String insertGongKinhSQL = "INSERT INTO gong_kinh (productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id, dt_expired, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateGongKinhSQL = "UPDATE gong_kinh SET dt_expired = '1999-01-01', is_active = 0 WHERE productId = ?";

        try (Connection conn = dbConnection.getDWConnection();
             PreparedStatement selectStmt = conn.prepareStatement(selectTempExistsChangeSQL);
             ResultSet rs = selectStmt.executeQuery()) {

            while (rs.next()) {
                int productId = rs.getInt("productId");
                Date localDate = rs.getDate("localDate");
                String imageURL = rs.getString("imageURL");
                String productURL = rs.getString("productURL");
                String productTitle = rs.getString("productTitle");
                float price = rs.getFloat("price");
                int discountPercentage = rs.getInt("discount_percentage");
                String brand = rs.getString("brand");
                String material = rs.getString("material");
                String style = rs.getString("style");
                String warranty = rs.getString("warranty");
                String colors = rs.getString("colors");
                int dateDimId = rs.getInt("date_dim_id");
                // Check if productId exists in gong_kinh
                try (PreparedStatement checkStmt = conn.prepareStatement(checkGongKinhSQL)) {
                    checkStmt.setInt(1, productId);
                    ResultSet gongKinhRs = checkStmt.executeQuery();

                    if (gongKinhRs.next()) {
                        // Update the existing row's dt_expired to 1999-01-01
                        try (PreparedStatement updateStmt = conn.prepareStatement(updateGongKinhSQL)) {
                            updateStmt.setInt(1, productId);
                            updateStmt.executeUpdate();
                        }
                        // Insert new record into gong_kinh with current date as dt_expired
                        try (PreparedStatement insertStmt = conn.prepareStatement(insertGongKinhSQL)) {
                            insertStmt.setInt(1, productId);
                            insertStmt.setDate(2, localDate);
                            insertStmt.setString(3, imageURL);
                            insertStmt.setString(4, productURL);
                            insertStmt.setString(5, productTitle);
                            insertStmt.setFloat(6, price);
                            insertStmt.setInt(7, discountPercentage);
                            insertStmt.setString(8, brand);
                            insertStmt.setString(9, material);
                            insertStmt.setString(10, style);
                            insertStmt.setString(11, warranty);
                            insertStmt.setString(12, colors);
                            insertStmt.setInt(13, dateDimId);
                            insertStmt.setDate(14, Date.valueOf(LocalDate.now())); // current date
                            insertStmt.setInt(15, 1); // assuming is_active is 1 for new records
                            insertStmt.executeUpdate();
                        }
                    }
                }
            }
            return true;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    public boolean insertAllToDW(List<TempExistsRecord> listTempExists) {
        String insertSQL = "INSERT INTO gong_kinh (productId, localDate, imageURL, productURL, productTitle, price, discount_percentage, brand, material, style, warranty, colors, date_dim_id, dt_expired, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dbConnection.getDWConnection();
             PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {

            for (TempExistsRecord record : listTempExists) {
                insertStmt.setInt(1, record.getProductId());
                insertStmt.setDate(2, record.getLocalDate());
                insertStmt.setString(3, record.getImageURL());
                insertStmt.setString(4, record.getProductURL());
                insertStmt.setString(5, record.getProductTitle());
                insertStmt.setFloat(6, record.getPrice());
                insertStmt.setInt(7, record.getDiscountPercentage());
                insertStmt.setString(8, record.getBrand());
                insertStmt.setString(9, record.getMaterial());
                insertStmt.setString(10, record.getStyle());
                insertStmt.setString(11, record.getWarranty());
                insertStmt.setString(12, record.getColors());
                insertStmt.setInt(13, record.getDateDimId());
                insertStmt.setString(14, String.valueOf(Date.valueOf(LocalDate.now())));
                insertStmt.setInt(15, 1);

                insertStmt.addBatch();
            }

            // Execute batch insert
            int[] results = insertStmt.executeBatch();
            System.out.println("Inserted " + results.length + " rows into DW (gong_kinh table).");
            return true;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void deleteAllTempNotExists() {
        String sql = "DELETE FROM temp_not_exists";
        try (Connection conn = dbConnection.getStagingConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Xóa dữ liệu thành công");
        } catch (SQLException e) {
            System.err.println("Thất bại: " + e.getMessage());
        }
    }

    public void deleteAllTempChange() {
        String sql = "DELETE FROM temp_exists_change";
        try (Connection conn = dbConnection.getStagingConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Xóa dữ liệu thành công");
        } catch (SQLException e) {
            System.err.println("Thất bại: " + e.getMessage());
        }
    }

    public void deleteAllTempExists() {
        String sql = "DELETE FROM temp_exists";
        try (Connection conn = dbConnection.getStagingConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Xóa dữ liệu thành công");
        } catch (SQLException e) {
            System.err.println("Thất bại: " + e.getMessage());
        }
    }
}
//    public static void main (String[]args){
//
////            DataService dataService = new DataService(new DatabaseConnection());
//////        dataService.bulkInsertProducts("D:\\DataWH\\DrawlData\\
//////            dataService.cleanAndTransferData();
////            dataService.syncTempExistsChangeToGongKinh();
////        }
//    }
