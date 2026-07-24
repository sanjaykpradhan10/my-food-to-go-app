package com.sanjay.ftgo.delivery.domain;

public record DeliveryScheduledEvent(Long orderId, Long courierId) implements DeliveryDomainEvent {
}
