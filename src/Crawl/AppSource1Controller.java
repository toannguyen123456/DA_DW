package Crawl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;
import java.io.IOException;

public class AppSource1Controller {

    private DataService dataService;

    public AppSource1Controller() {}

    public AppSource1Controller(DataService dataService) {
        this.dataService = dataService;
    }
    public void startDataProcess() {
        String[] configData = getPath();
        if (configData != null && configData.length == 3) {
            int id = Integer.parseInt(configData[0]);
            String baseUrl = configData[1];
            String directoryPath = configData[2];
            CrawlDataSource1 data = new CrawlDataSource1(id, baseUrl, directoryPath);
            boolean isDataCrawl = data.drawData();
            if(isDataCrawl) {
              String fileName = data.getCsvFileName();
              String status = "ER";
              int count = data.getRowCount();
              long size = data.getFileSize();
              dataService.updateFileLog(id, fileName, status, count, size);
            }else {
                System.out.println("không thể lấy được dữ liệu");
            }
        } else {
            System.out.println("không lấy được dữ liệu.");
        }
    }

    public static void main(String[] args) {
        String configFilePath = "D:\\DataWH\\code\\code\\src\\Crawl\\config.json";
        AppSource1Controller appSource1Controller = new AppSource1Controller();
        DatabaseConnection dbConnection = appSource1Controller.openConnection(configFilePath);
        DataService dataService = new DataService(dbConnection);
        AppSource1Controller appController = new AppSource1Controller(dataService);
        // 1.crawl data
        appController.startDataProcess();

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
     * 3.Lấy thư mục chứa file
     * (Cột source_file trong Control.config_file)
     * @return
     */
    public String[] getPath() {
        String[] configData = dataService.getFileConfigData(1);
        if(configData != null && configData.length > 0) {
            System.out.println("thành công");
        }else {
            System.out.println("lỗi khi lấy thư mực");
        }
        return configData;
    }
}
