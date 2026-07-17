package com.sanjay.ftgo.accounting.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "saga_join_state")
public class SagaJoinState {

    @Id
    private Long orderId;

    private boolean consumerVerified;
    private boolean ticketCreated;
    private boolean failed;
    private boolean resolved;
    private Integer totalQuantity;

    @Version
    private Long version;

    protected SagaJoinState() {
    }

    public SagaJoinState(Long orderId) {
        this.orderId = orderId;
        this.consumerVerified = false;
        this.ticketCreated = false;
        this.failed = false;
        this.resolved = false;
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

    public boolean isResolved() {
        return resolved;
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

    public void markTicketCreated(Integer totalQuantity) {
        this.ticketCreated = true;
        this.totalQuantity = totalQuantity;
    }

    public void markFailed() {
        this.failed = true;
    }

    public void markResolved() {
        this.resolved = true;
    }
}
