package Crawl;

public class AppSource2Controller {

    private DataService dataService;

    public AppSource2Controller(DataService dataService) {
        this.dataService = dataService;
    }
    public void startDataProcess() {
        String[] configData = dataService.getFileConfigData(1);

        if (configData != null && configData.length == 3) {
            int id = Integer.parseInt(configData[0]);
            String baseUrl = configData[1];
            String directoryPath = configData[2];
            CrawlDataSource1 data = new CrawlDataSource1(id, baseUrl, directoryPath);
            boolean isDataCrawl = data.drawData();
            String csvfile = data.getCsvFileName();
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

//    public static void main(String[] args) {
//        DatabaseConnection dbConnection = new DatabaseConnection();
//        DataService dataService = new DataService(dbConnection);
//        AppSource2Controller appController = new AppSource2Controller(dataService);
//        appController.startDataProcess();
//    }
}
