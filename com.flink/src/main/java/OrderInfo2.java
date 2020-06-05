public class OrderInfo2 {

    public static OrderInfo2 line2Info2(String line) {
        //需要判断line的信息内容
        String[] field = line.split(",");
        OrderInfo2 orderInfo2 = new OrderInfo2();
        orderInfo2.setOrderId(Long.parseLong(field[0]));
        orderInfo2.setOrderDate(field[1]);
        orderInfo2.setArea(field[2]);
        return orderInfo2;
    }
    private Long orderId;
    private String orderDate;
    private String area;

    public OrderInfo2(Long orderId, String orderDate, String area) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.area = area;
    }

    public OrderInfo2() {

    }

    public Long getOrderId() {
        return orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public String getArea() {
        return area;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public void setArea(String area) {
        this.area = area;
    }

    @Override
    public String toString() {
        return "OrderInfo2{" +
                "orderId=" + orderId +
                ", orderDate='" + orderDate + '\'' +
                ", area='" + area + '\'' +
                '}';
    }
}
