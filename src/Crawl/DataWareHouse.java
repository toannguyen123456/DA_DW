package Crawl;

import model.TempExistsRecord;

import java.util.List;

public class DataWareHouse {
    private DataService dataService;
    public DataWareHouse(DataService dataService) {
        this.dataService = dataService;
    }

    public void dataWareHouse() {

        boolean checkData = dataService.checkAndInsertData();
        if(checkData) {
            System.out.println("thêm vào bảng tạm thành công");
            List<TempExistsRecord> listTempxixists = dataService.getAllTempExistsData();
            List<TempExistsRecord> listChange = dataService.getAllTempExistsChange();
            if (listTempxixists.size() > 0) {
                System.out.println("thêm dữ liệu vào bảng temps_exists");
                boolean checkIsertTempExists = dataService.insertAllToDW(listTempxixists);
//                dataService.deleteAllTempExists();
            }

            if(listChange.size() > 0) {
                System.out.println("thêm dữ liệu vào bảng temps_exists_change");
                boolean checkChange = dataService.syncTempExistsChangeToGongKinh();
//                dataService.deleteAllTempChange();
            }
            dataService.deleteAllTempNotExists();

        }else  {
            System.out.println("thêm vào bảng tạm thất bại");
        }


    }
//    public static void main(String[] args) {
//
//        DatabaseConnection dbConnection = new DatabaseConnection();
//        DataService dataService = new DataService(dbConnection);
//        DataWareHouse dataStaging = new DataWareHouse(dataService);
//        dataStaging.dataWareHouse();
//
//    }

}
