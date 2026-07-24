package com.sanjay.ftgo.delivery.domain;

public record DeliveryPickedUpEvent(Long orderId) implements DeliveryDomainEvent {
}
