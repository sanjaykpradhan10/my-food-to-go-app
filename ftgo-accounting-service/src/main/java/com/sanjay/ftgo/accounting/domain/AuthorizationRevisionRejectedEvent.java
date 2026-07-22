package com.sanjay.ftgo.accounting.domain;

public record AuthorizationRevisionRejectedEvent(Long orderId, String reason) implements AuthorizationDomainEvent {
}
