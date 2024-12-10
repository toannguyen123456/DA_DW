package Crawl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;
import java.io.IOException;

public class TransformSource1 {
    private DataService dataService ;
    public TransformSource1() {
    }
    public TransformSource1(DataService dataService) {
        this.dataService = dataService;
    }

    private void transform() {
        if(checkStatusTR()) {
            boolean cleanData = performTransform();
            if (cleanData) {
                boolean cleanAndTransferData = loadToProductsDaily();
                if (cleanAndTransferData) {
                    dataService.deleteAllProducts();
                }
                checkStatusSC();

            } else {
                System.out.println("transform không thành công");
            }
        }else {
            System.out.println("không tồn tại Status TR:");
        }

    }

    public static void main(String[] args) {
        String configFilePath = "D:\\DataWH\\code\\code\\src\\Crawl\\config.json";
        TransformSource1 transformSource1 = new TransformSource1();
        DatabaseConnection dbConnection = transformSource1.openConnection(configFilePath);
        DataService dataService = new DataService(dbConnection);
        TransformSource1 dataStaging = new TransformSource1(dataService);
        dataStaging.transform();

    }

    /**
     * 9. Kiểm tra record file_log
     * @return
     */
    private boolean checkStatusTR() {
        String status = dataService.getStatus("TR");
        if ("TR".equals(status)) {
            return true;
        } else {
            System.out.println("Không tồn tại Status TR.");
            return false;
        }
    }

    /**
     * 10. Thực hiện transform dữ liệu
     * @return
     */
    public boolean performTransform() {
        boolean transform  = dataService.cleanData();
        if(transform) {
            System.out.println("transform thành công");
        }else {
            System.out.println("transform thất bại");
        }
        return transform;
    }

    /**
     * 11. Load dữ liệu vào bảng products_daily_1
     * @return
     */
    private boolean loadToProductsDaily() {
        boolean loadSuccess = dataService.cleanAndTransferData();
        if (loadSuccess) {
            System.out.println("Dữ liệu đã được load vào table products_daily_1 thành công.");
        } else {
            System.out.println("Lỗi khi load dữ liệu vào table products_daily_1.");
        }
        return loadSuccess;
    }

    /**
     *  12. Cập nhật status thành SC
     */
    public void checkStatusSC(){
        String status = dataService.getStatus("TR");
        if ("TR".equals(status)) {
            boolean b = dataService.updateAllStatusTRtoSC();
        }
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

}
