package Crawl;

public class Product {

    private int productId;
    private String imageURL;
    private String productURL;
    private String productTitle;
    private String price;
    private String discountPercentage;
    private String originalPrice;
    private String brand;
    private String material;
    private String style;
    private String warranty;
    private String colors;

    // Constructor
    public Product(int productId, String imageURL, String productURL, String productTitle, String price,
                   String discountPercentage, String originalPrice, String brand, String material,
                   String style, String warranty, String colors) {
        this.productId = productId;
        this.imageURL = imageURL;
        this.productURL = productURL;
        this.productTitle = productTitle;
        this.price = price;
        this.discountPercentage = discountPercentage;
        this.originalPrice = originalPrice;
        this.brand = brand;
        this.material = material;
        this.style = style;
        this.warranty = warranty;
        this.colors = colors;
    }

    // Getters
    public int getProductId() { return productId; }
    public String getImageURL() { return imageURL; }
    public String getProductURL() { return productURL; }
    public String getProductTitle() { return productTitle; }
    public String getPrice() { return price; }
    public String getDiscountPercentage() { return discountPercentage; }
    public String getOriginalPrice() { return originalPrice; }
    public String getBrand() { return brand; }
    public String getMaterial() { return material; }
    public String getStyle() { return style; }
    public String getWarranty() { return warranty; }
    public String getColors() { return colors; }
}
