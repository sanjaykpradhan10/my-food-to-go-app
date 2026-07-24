package com.sanjay.ftgo.delivery.domain;

public record DeliverySchedulingFailedEvent(Long orderId, String reason) implements DeliveryDomainEvent {
}
