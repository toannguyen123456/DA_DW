package Crawl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DataStagingSource1 {

    private DataService dataService ;
    public DataStagingSource1() {
    }

    public void startDataStaging(String time) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date;
        if (time != null) {
            try {
                date = LocalDate.parse(time, formatter);
            } catch (DateTimeParseException e) {
                System.out.println("Định dạng ngày tháng không hợp lệ: " + time);
                return;
            }
        } else {
            date = LocalDate.now();
        }
        // 2. Kiểm tra record trạng thái ER
        String formattedDate = date.format(formatter);
        String[] result = checkERRecord(formattedDate);

        if(result == null){
            return;
        }
        String fileName = getFileName(formattedDate);
        System.out.println(fileName);
        if (fileName == null) {
            return;
        }
        int configId = Integer.parseInt(result[1]);
        String sourceFileLocation = result[3];
        if (!updateStatusToProcessing(configId)) {
            return;
        }
        processFile(fileName, sourceFileLocation, configId);

    }

    public static void main(String[] args) {
        String configFilePath = "D:\\DataWH\\code\\code\\src\\Crawl\\config.json";
        DataStagingSource1 stagingInstance = new DataStagingSource1();
        DatabaseConnection dbConnection = stagingInstance.openConnection(configFilePath);

        if (dbConnection != null) {
            DataService dataService = new DataService(dbConnection);
            DataStagingSource1 dataStaging = new DataStagingSource1(dataService);
            dataStaging.startDataStaging("2024-12-10");
        } else {
            System.err.println("Không thể khởi tạo kết nối cơ sở dữ liệu");
        }
    }
    public DataStagingSource1(DataService dataService) {
        this.dataService = dataService;
    }

    /**
     * 1. Load config.json
     * @param configFilePath đường dẫn đến file config.json
     * @return
     */
    private JsonObject loadConfig(String configFilePath) {
        try (FileReader reader = new FileReader(configFilePath)) {
            return JsonParser.parseReader(reader).getAsJsonObject();
        } catch (IOException e) {
            System.err.println("Không thể đọc tệp cấu hình: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     *  2. kết nối với db.control  và 6. db.Staging
     * @param configFilePath đường dẫn đến file config.json
     * @return
     */
    public DatabaseConnection openConnection(String configFilePath) {
        JsonObject config = loadConfig(configFilePath);
        if (config != null) {
            return new DatabaseConnection(config);
        }else {
            System.out.println("không kết nối được database");
        }
        return null;
    }

    /**
     *  3. kiểm tra record trong db.control.file_log
     * @param date ngày tháng năm
     * @return
     */
    private String[] checkERRecord(String date) {
        String[] result = dataService.getFirstStatus(date);
        System.out.println(result[0]);
        if (result == null || result[0] == null || !result[0].equals("ER")) {
            System.out.println("Không tìm thấy status ER.");
            return null;
        }
        return result;
    }

    /**
     *  4. cập nhật status thành PS
     * @param configId id file_log
     * @return
     */
    private boolean updateStatusToProcessing(int configId) {
        try {
            dataService.updateStatus(configId, "PS");
            System.out.println("Đã cập nhật trạng thái thành PS.");
            return true;
        } catch (Exception e) {
            System.err.println("Lỗi khi cập nhật trạng thái: " + e.getMessage());
            return false;
        }
    }

    /**
     * 5. lấy ra fileName thỏa mảng điều kiện
     * @param date năm tháng ngày
     * @return
     */
    private String getFileName(String date) {
        String[] result = dataService.getFirstStatus(date);
        if (result == null || result[2] == null) {
            System.err.println("Không tìm thấy fileName hợp lệ.");
            return null;
        }
        return result[2];
    }

    /**
     * 7. load dữ liệu vào bảng product_temps_1
     * @param csvFile fileName.csv
     * @param sourceFileLocation thư mục lưu
     * @param configId id  file_log
     */
    private void processFile(String csvFile, String sourceFileLocation, int configId) {
        if (csvFile == null || sourceFileLocation == null) {
            System.err.println("Dữ liệu filename hoặc đường dẫn không hợp lệ.");
            dataService.updateStatus(configId, "FL");
            return;
        }
        String csvFilePath = sourceFileLocation + "\\" + csvFile;
        System.out.println("Đường dẫn file: " + csvFilePath);

        try {
            String url = csvFilePath.replace("\\\\", "\\");
            dataService.bulkInsertProducts(url);
            updateStatusToSuccess(configId);
            System.out.println("Dữ liệu đã được thêm thành công.");
        } catch (Exception e) {
            System.err.println("Lỗi khi thêm dữ liệu: " + e.getMessage());
            dataService.updateStatus(configId, "FL");
        }
    }

    /**
     * 8. cập nhật status thành TR
     * @param configId id file_log
     */
    private void updateStatusToSuccess(int configId) {
        try {
            dataService.updateStatus(configId, "TR");
            System.out.println("Đã cập nhật trạng thái thành TR.");
        } catch (Exception e) {
            System.err.println("Lỗi khi cập nhật trạng thái TR: " + e.getMessage());
        }
    }
}
