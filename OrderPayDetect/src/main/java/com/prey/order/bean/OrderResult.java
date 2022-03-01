package com.prey.order.bean;

import lombok.Data;

@Data
public class OrderResult {
    private Long orderId;
    private String resultState;

    public OrderResult(Long orderId, String resultState) {
        this.orderId = orderId;
        this.resultState = resultState;
    }
}
