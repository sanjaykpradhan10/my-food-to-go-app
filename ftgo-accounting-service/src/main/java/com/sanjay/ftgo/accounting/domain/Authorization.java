package com.sanjay.ftgo.accounting.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.List;

@Entity
@Table(name = "authorizations")
public class Authorization {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    @Enumerated(EnumType.STRING)
    private AuthorizationStatus status;

    private int totalQuantity;

    protected Authorization() {
    }

    private Authorization(Long orderId, AuthorizationStatus status, int totalQuantity) {
        this.orderId = orderId;
        this.status = status;
        this.totalQuantity = totalQuantity;
    }

    public static AuthorizationResult authorize(Long orderId, int totalQuantity) {
        Authorization authorization = new Authorization(orderId, AuthorizationStatus.AUTHORIZED, totalQuantity);
        return new AuthorizationResult(authorization, List.of(new CardAuthorizedEvent(orderId)));
    }

    public static AuthorizationResult decline(Long orderId, String reason, int totalQuantity) {
        Authorization authorization = new Authorization(orderId, AuthorizationStatus.DECLINED, totalQuantity);
        return new AuthorizationResult(authorization, List.of(new CardAuthorizationDeclinedEvent(orderId, reason)));
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public AuthorizationStatus getStatus() {
        return status;
    }

    public int getTotalQuantity() {
        return totalQuantity;
    }

    public List<AuthorizationDomainEvent> reverse() {
        if (status != AuthorizationStatus.AUTHORIZED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = AuthorizationStatus.REVERSED;
        return List.of(new AuthorizationReversedEvent(orderId));
    }

    public List<AuthorizationDomainEvent> reviseAuthorization(int newTotalQuantity) {
        if (status != AuthorizationStatus.AUTHORIZED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.totalQuantity = newTotalQuantity;
        return List.of(new AuthorizationRevisedEvent(orderId, newTotalQuantity));
    }
}
