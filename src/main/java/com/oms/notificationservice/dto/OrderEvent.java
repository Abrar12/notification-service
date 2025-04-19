package com.oms.notificationservice.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OrderEvent {
    private String orderId;
    private String status;
    private String message;

    public OrderEvent() {}

    public OrderEvent(String orderId, String status, String message) {
        this.orderId = orderId;
        this.status = status;
        this.message = message;
    }
}