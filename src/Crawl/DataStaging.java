package Crawl;

public class DataStaging {

    private DataService dataService ;

    public DataStaging(DataService dataService) {
        this.dataService = dataService;
    }


    public void startDataStaging() {
        String[] result = dataService.getFirstStatus();
        String status = result[0];
        int configId = Integer.parseInt(result[1]) ;
        String csvFile = result[2];
        String sourceFileLocation = result[3];
        String csvFilePath  = sourceFileLocation + "\\\\" + csvFile;


        System.out.println(sourceFileLocation);
        System.out.println(csvFilePath);

        if(status.equals("ER")) {
           if(csvFile != null) {
               dataService.updateStatus(configId, "PS");
                if (csvFilePath != null) {
                    String url = csvFilePath.replace("\\\\", "\\");
                    dataService.bulkInsertProducts(url);
                    dataService.updateStatus(configId, "SC");
                } else {
                    dataService.updateStatus(configId, "FL");
                }
           }else {
               System.out.println("không lấy được dữ liệu csv");
           }


        }else {
            System.out.println("lỗi không lấy được status");
        }
    }

    public static void main(String[] args) {


        DatabaseConnection dbConnection = new DatabaseConnection();
        DataService dataService = new DataService(dbConnection);
        DataStaging dataStaging = new DataStaging(dataService);
        dataStaging.startDataStaging();


    }

}
