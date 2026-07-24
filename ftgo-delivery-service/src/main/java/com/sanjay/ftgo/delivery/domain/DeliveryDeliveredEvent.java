package com.sanjay.ftgo.delivery.domain;

public record DeliveryDeliveredEvent(Long orderId) implements DeliveryDomainEvent {
}
