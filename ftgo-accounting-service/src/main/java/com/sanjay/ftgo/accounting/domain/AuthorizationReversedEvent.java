package com.sanjay.ftgo.accounting.domain;

public record AuthorizationReversedEvent(Long orderId) implements AuthorizationDomainEvent {
}
