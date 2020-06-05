public class OrderInfo1 {

    public static OrderInfo1 line2Info1(String line) {
        //需要判断line的信息内容
        String[] field = line.split(",");
        OrderInfo1 orderInfo1 = new OrderInfo1();
        orderInfo1.setOrderId(Long.parseLong(field[0]));
        orderInfo1.setProductName(field[1]);
        orderInfo1.setPrice(Double.parseDouble(field[2]));
        return orderInfo1;
    }

    private Long orderId;

    private String productName;

    private  Double price;

    public OrderInfo1(Long orderId, String name, Double p) {
        this.orderId = orderId;
        this.productName = name;
        this.price = p;
    }

    public OrderInfo1() {

    }

    @Override
    public String toString() {
        return "OrderInfo1{" +
                "orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getProductName() {
        return productName;
    }

    public Double getPrice() {
        return price;
    }
}
