package model;


import java.sql.Date;

public class TempExistsRecord {

    private int productId;
    private Date localDate;
    private String imageURL;
    private String productURL;
    private String productTitle;
    private float price;
    private int discountPercentage;
    private String brand;
    private String material;
    private String style;
    private String warranty;

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public String getColors() {
        return colors;
    }

    public void setColors(String colors) {
        this.colors = colors;
    }

    public Date getLocalDate() {
        return localDate;
    }

    public void setLocalDate(Date localDate) {
        this.localDate = localDate;
    }

    public String getImageURL() {
        return imageURL;
    }

    public void setImageURL(String imageURL) {
        this.imageURL = imageURL;
    }

    public String getProductURL() {
        return productURL;
    }

    public void setProductURL(String productURL) {
        this.productURL = productURL;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getDiscountPercentage() {
        return discountPercentage;
    }

    public void setDiscountPercentage(int discountPercentage) {
        this.discountPercentage = discountPercentage;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getMaterial() {
        return material;
    }

    public void setMaterial(String material) {
        this.material = material;
    }

    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }

    public String getWarranty() {
        return warranty;
    }

    public void setWarranty(String warranty) {
        this.warranty = warranty;
    }

    public int getDateDimId() {
        return dateDimId;
    }

    public void setDateDimId(int dateDimId) {
        this.dateDimId = dateDimId;
    }

    private String colors;
    private int dateDimId;

    public TempExistsRecord(int productId, Date localDate, String imageURL, String productURL, String productTitle, float price, int discountPercentage, String brand, String material, String style, String warranty, String colors, int dateDimId) {
        this.productId = productId;
        this.localDate = localDate;
        this.imageURL = imageURL;
        this.productURL = productURL;
        this.productTitle = productTitle;
        this.price = price;
        this.discountPercentage = discountPercentage;
        this.brand = brand;
        this.material = material;
        this.style = style;
        this.warranty = warranty;
        this.colors = colors;
        this.dateDimId = dateDimId;
    }

}

