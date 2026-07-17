package com.sanjay.ftgo.order.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "create_order_saga_instances")
public class CreateOrderSagaInstance {

    @Id
    private Long orderId;

    private boolean consumerVerified;
    private boolean ticketCreated;
    private boolean failed;
    private Integer totalQuantity;

    @Version
    private Long version;

    protected CreateOrderSagaInstance() {
    }

    public CreateOrderSagaInstance(Long orderId, Integer totalQuantity) {
        this.orderId = orderId;
        this.totalQuantity = totalQuantity;
        this.consumerVerified = false;
        this.ticketCreated = false;
        this.failed = false;
    }

    public Long getOrderId() {
        return orderId;
    }

    public boolean isConsumerVerified() {
        return consumerVerified;
    }

    public boolean isTicketCreated() {
        return ticketCreated;
    }

    public boolean isFailed() {
        return failed;
    }

    public Integer getTotalQuantity() {
        return totalQuantity;
    }

    public Long getVersion() {
        return version;
    }

    public void markConsumerVerified() {
        this.consumerVerified = true;
    }

    public void markTicketCreated() {
        this.ticketCreated = true;
    }

    public void markFailed() {
        this.failed = true;
    }
}
