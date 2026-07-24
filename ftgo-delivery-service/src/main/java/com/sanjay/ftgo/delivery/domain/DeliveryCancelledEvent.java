package com.sanjay.ftgo.delivery.domain;

public record DeliveryCancelledEvent(Long orderId) implements DeliveryDomainEvent {
}
