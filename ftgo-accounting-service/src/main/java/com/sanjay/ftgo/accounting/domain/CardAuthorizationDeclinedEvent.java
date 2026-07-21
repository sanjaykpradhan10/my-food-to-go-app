package com.sanjay.ftgo.accounting.domain;

public record CardAuthorizationDeclinedEvent(Long orderId, String reason) implements AuthorizationDomainEvent {
}
