package com.wubaibao.flinkjava.code.chapter10.example;

/**
 * OrderInfo :订单信息
 *  orderId:订单ID
 *  orderAmount: 订单金额
 *  orderTime:订单时间
 *  payState: 支付状态
 */
public class OrderInfo {
    public String orderId;
    public Double orderAmount;
    public Long orderTime;
    public String payState;

    public OrderInfo(String orderId, Double orderAmount, Long orderTime, String payState) {
        this.orderId = orderId;
        this.orderAmount = orderAmount;
        this.orderTime = orderTime;
        this.payState = payState;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String uid) {
        this.orderId = orderId;
    }

    public Double getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(Double userName) {
        this.orderAmount = orderAmount;
    }

    public Long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(Long loginTime) {
        this.orderTime = orderTime;
    }

    public String getPayState() {
        return payState;
    }

    public void setPayState(String lognState) {
        this.payState = payState;
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "orderId='" + orderId + '\'' +
                ", orderAmount='" + orderAmount + '\'' +
                ", orderTime=" + orderTime +
                ", payState='" + payState + '\'' +
                '}';
    }
}
