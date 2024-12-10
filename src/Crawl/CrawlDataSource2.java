package Crawl;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CrawlDataSource2 {

    private String directoryPath;
    private String csvFileName;
    private String baseUrl;
    private int productId;
//,
    public CrawlDataSource2(int productId, String baseUrl, String directoryPath) {
        this.directoryPath = directoryPath;
        this.baseUrl = baseUrl;
        this.productId = productId;
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        this.csvFileName = "products_2" + timestamp + ".csv";
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


    public boolean crawlData() throws IOException {

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

        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String url = this.getBaseUrl() + "collections/kinh-gong";


        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(directoryPath + "\\" + this.getCsvFileName()), StandardCharsets.UTF_8))) {
            writer.write("\uFEFF");
            writer.write("productId, date,imageURL,productURL,productTitle,price,discount percentage,brand,material,style,warranty,colors\n");

            int totalProductsPage = 0;
            Document doc = Jsoup.connect(url).get();
            Element element = doc.selectFirst("span.t4s-lm-bar--txt");

            if (element != null) {
                String text = element.text();
                String[] parts = text.split("/");
                totalProductsPage = Integer.parseInt(parts[1].replaceAll("[^0-9]", ""));

            }
            for(int i = 1; i < 5;  i++) {
                Document pageDoc = Jsoup.connect(this.getBaseUrl() + "/collections/kinh-gong?page=" + i).get();
                Elements products = pageDoc.select("div.t4s-product-wrapper");

                for (Element productElement : products) {
                    // Lấy tên sản phẩm
                    Element nameElement = productElement.selectFirst("h3.t4s-product-title a");
                    String productName = nameElement != null ? nameElement.text() : "No name";

    //                giá đã giảm giá
                    Element priceElement = productElement.selectFirst("div.t4s-product-price ins");
                    String currentPriceText = priceElement != null ? priceElement.text().replaceAll("[^0-9]", "") : "0";
                    double currentPrice = Double.parseDouble(currentPriceText);

                    // Lấy giá gốc
                    Element originalPriceElement = productElement.selectFirst("div.t4s-product-price del");
                    String originalPriceText = originalPriceElement != null ? originalPriceElement.text().replaceAll("[^0-9]", "") : "0";
                    double originalPrice = Double.parseDouble(originalPriceText);

                    double discountPercentage = 0;
                    String strPrice = "";

                    if(priceElement != null || originalPriceElement != null) {
                        discountPercentage = ((originalPrice - currentPrice) / originalPrice) * 100;
                    }

                    if (originalPriceElement != null) {
                        strPrice = cleanPrice(originalPriceText);
                    } else {
                        Element price = productElement.selectFirst("div.t4s-product-price");
                        long checkPrice = Long.parseLong(price.text().replaceAll("[^0-9]", ""));
                        if (checkPrice < 100000000) {
                            strPrice = cleanPrice(price.text());
                            System.out.println(strPrice);
                        } else {
                            strPrice = "";
                        }
                    }
                    // Lấy link ảnh
                    Element noscriptElement = productElement.selectFirst("noscript img.t4s-product-main-img");
                    String imageUrl = noscriptElement != null ? noscriptElement.attr("src") : "No image URL";

                    // Lấy link thể loại
                    Element categoryLinkElement = productElement.selectFirst("a.t4s-full-width-link");
                    String productUrl = categoryLinkElement != null ? "https://eyewearhut.vn/" + categoryLinkElement.attr("href") : "No category URL";
                    String[] productDetails = scrapeProductDetails(productUrl);

                    String brand = escapeCSV(productDetails[0]);
                    String material = escapeCSV(productDetails[1]);
                    String frameType = escapeCSV(productDetails[2]);
                    String style = escapeCSV(productDetails[3]);
                    String color = escapeCSV(productDetails[4]);

                    writer.write(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
                            this.getProductId(), date, imageUrl, productUrl, productName, strPrice, discountPercentage , brand, material, frameType, style,  color));
                }
            }
            return false;

        } catch (IOException e) {
            System.err.println("không ghi vào csv: " + e.getMessage());
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
        return price.replaceAll("[^\\d]", "").trim();
    }

    public static String[] scrapeProductDetails(String productUrl) throws IOException {
        Document doc = Jsoup.connect(productUrl).get();

        List<Element> listItems = doc.select("ul li");
        String material = "";
        String frameType = "";
        String style = "";
        String brand = "";

        for (Element item : listItems) {
            String itemText = item.text();
            if (itemText.contains("Chất liệu")) {
                material = itemText.replace("Chất liệu: ", "").trim();
            } else if (itemText.contains("Kiểu gọng")) {
                frameType = itemText.replace("Kiểu gọng: ", "").trim();
            } else if (itemText.contains("Phong cách")) {
                style = itemText.replace("Phong cách: ", "").trim();
            }
        }
        Element colorElement = doc.select("h4.t4s-swatch__title span.t4s-dib.t4s-swatch__current").first();
        String color = colorElement != null ? colorElement.text().trim() : "No color info";

        Element brandElement = doc.select("div:has(span.product_headline:contains(Thương hiệu)) + div a").first();
        if (brandElement != null) {
            brand = brandElement.text().trim();
        }
        return new String[]{brand, material, frameType, style, color};
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

    public static void main(String[] args) throws IOException {
        CrawlDataSource2 crawlDataSource2 = new CrawlDataSource2(1, "https://eyewearhut.vn/", "D:\\DataWH\\DrawlData");
        crawlDataSource2.crawlData();

    }
}
