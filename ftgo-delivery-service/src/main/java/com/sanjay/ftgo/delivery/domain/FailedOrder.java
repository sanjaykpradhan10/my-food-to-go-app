package com.sanjay.ftgo.delivery.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "failed_orders")
public class FailedOrder {

    @Id
    private Long orderId;

    protected FailedOrder() {
    }

    public FailedOrder(Long orderId) {
        this.orderId = orderId;
    }

    public Long getOrderId() {
        return orderId;
    }
}
