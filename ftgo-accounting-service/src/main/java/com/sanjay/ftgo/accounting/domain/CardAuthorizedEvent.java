package com.sanjay.ftgo.accounting.domain;

public record CardAuthorizedEvent(Long orderId) implements AuthorizationDomainEvent {
}
