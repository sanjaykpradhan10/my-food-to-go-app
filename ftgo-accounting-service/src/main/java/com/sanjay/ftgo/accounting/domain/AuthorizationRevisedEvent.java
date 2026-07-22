package com.sanjay.ftgo.accounting.domain;

public record AuthorizationRevisedEvent(Long orderId, int totalQuantity) implements AuthorizationDomainEvent {
}
