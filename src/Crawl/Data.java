package Crawl;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Data {

    private String directoryPath;
    private String csvFileName;
    private String baseUrl;
    private int productId;
    private int price;
    private int discountPercentage;

    public Data(int productId,String baseUrl, String directoryPath) {
        this.directoryPath = directoryPath;
        this.baseUrl = baseUrl;
        this.productId = productId;
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        this.csvFileName = "products_" + timestamp + ".csv";
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public String getDirectoryPath() {
        return directoryPath;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public int getProductId() {
        return productId ++;
    }

    public boolean drawData() {

        String imageUrl = "";
        String url = this.getBaseUrl() + "/collections/gong-kinh";

        File directory = new File(this.getDirectoryPath());

        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("tạo thư mục thành công: " + directoryPath);
            } else {
                System.out.println("tạo thư mục không thành công: " + directoryPath);
                return false;
            }
        } else {
            System.out.println("thư mục: " + directoryPath);
        }


        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(directoryPath + "\\" + this.getCsvFileName()), StandardCharsets.UTF_8))) {
            writer.write("\uFEFF");

            writer.write("productId,imageURL,productURL,productTitle,price,discount percentage,brand,material,style,warranty,colors\n");

            Document doc = Jsoup.connect(url).get();
            Element page = doc.select("a[class=page-node]").last();
            String lastPageText = page.ownText();
            int lastPageNumber = Integer.parseInt(lastPageText);
//            int halfLastPageNumber = lastPageNumber / 2;

            for (int i = 1; i <= 3; i++) {
                Document pageDoc = Jsoup.connect(this.getBaseUrl() + "/collections/gong-kinh?page=" + i).get();
                Elements products = pageDoc.select("div.product-block");
                System.out.println(i);
                for (Element product : products) {
                    Element firstSource = product.select("picture source").first();
                    if (firstSource != null) {
                        imageUrl = firstSource.attr("srcset");
                        if (imageUrl.startsWith("//")) {
                            imageUrl = "https:" + imageUrl;
                        }
                    }

                    Element detailsLink = product.select("div.over-detail a").first();
                    String productHref = detailsLink != null ? detailsLink.attr("href") : "";
                    String productUrl = this.getBaseUrl() + productHref;
                    String[] productDetails = scrapeProductDetails(productUrl);

                    String productTitle = escapeCSV(productDetails[0]);
                    if(!cleanPrice(productDetails[1]).equals("")){
                        price = Integer.parseInt(cleanPrice(productDetails[1]));
                    }

                    if (!cleanPrice(productDetails[2]).equals("")){
                        discountPercentage = Integer.parseInt(cleanPrice(productDetails[2]));
                    }

                    String brand = escapeCSV(productDetails[3]);
                    String material = escapeCSV(productDetails[4]);
                    String style = escapeCSV(productDetails[5]);
                    String warranty = escapeCSV(productDetails[6]);
                    String colors = escapeCSV(productDetails[7]);

                    writer.write(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
                            this.getProductId(), imageUrl, productUrl, productTitle, price, discountPercentage , brand, material, style, warranty, colors));
                }
            }
            System.out.println("lưu dữ liệu:  " + directoryPath + "\\" + csvFileName + " thành công!");
            return true;
        } catch (IOException e) {
            System.err.println("không ghi vào csv: " + e.getMessage());
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static String[] scrapeProductDetails(String productUrl) throws IOException {
        Document productDoc = Jsoup.connect(productUrl).get();

        Element titleElement = productDoc.select("h1").first();
        String productTitle = titleElement != null ? titleElement.text() : "No product title available";

        Element priceElement = productDoc.select("#price-preview .pro-price").first();
        String price = priceElement != null ? priceElement.text() : "No price available";

        Element saleElement = productDoc.select("#price-preview .pro-sale").first();
        String salePercentage = saleElement != null ? saleElement.text() : "No discount available";



        Elements detailsElements = productDoc.select(".motangan p");
        String brand = "", material = "", style = "", warranty = "";

        for (Element detail : detailsElements) {
            String text = detail.text();
            if (text.startsWith("Thương Hiệu: ")) {
                brand = text.replace("Thương Hiệu: ", "").trim();
            } else if (text.startsWith("Chất Liệu Gọng: ")) {
                material = text.replace("Chất Liệu Gọng: ", "").trim();
            } else if (text.startsWith("Kiểu Dáng: ")) {
                style = text.replace("Kiểu Dáng: ", "").trim();
            } else if (text.startsWith("Thời Gian Bảo Hành: ")) {
                warranty = text.replace("Thời Gian Bảo Hành: ", "").trim();
            }

        }

        Elements colorOptions = productDoc.select(".swatch-element");
        StringBuilder colors = new StringBuilder();
        for (Element color : colorOptions) {
            String colorName = color.attr("data-value");
            colors.append(colorName).append("-");
        }
        return new String[]{productTitle, price, salePercentage, brand, material, style, warranty, colors.toString(),};
    }

    private static String escapeCSV(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }

        if (value.endsWith("-")) {
            value = value.replaceAll("-$", "");
        }
        return value;
    }

    private static String cleanPrice(String price) {
        if (price == null || price.trim().isEmpty()) {
            return "";
        }

        return price.replaceAll("[^\\d.]", "").trim();
    }



    public long getFileSize() {
        File fileSize = new File(directoryPath + "\\" + csvFileName);
        return fileSize.length();
    }

    public int getRowCount() {
        int rowCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(directoryPath + "\\" + csvFileName))) {
            reader.readLine();
            while (reader.readLine() != null) {
                rowCount++;
            }
        } catch (IOException e) {
            System.err.println("Lỗi khi đếm dòng: " + e.getMessage());
        }
        return rowCount;
    }



}