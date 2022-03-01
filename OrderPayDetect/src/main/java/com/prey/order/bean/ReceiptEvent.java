package com.prey.order.bean;

import lombok.Data;

@Data
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long timestamp;

    public ReceiptEvent(String txId, String payChannel, Long timestamp) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.timestamp = timestamp;
    }
}
