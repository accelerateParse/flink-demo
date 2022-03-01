package com.prey.order.bean;

import lombok.Data;

@Data
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long timestamp;

    public OrderEvent(Long orderId, String eventType, String txId, Long timestamp) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.timestamp = timestamp;
    }
}
