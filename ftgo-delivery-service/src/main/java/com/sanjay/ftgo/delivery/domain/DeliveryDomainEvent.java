package com.sanjay.ftgo.delivery.domain;

public sealed interface DeliveryDomainEvent
        permits DeliveryScheduledEvent, DeliverySchedulingFailedEvent, DeliveryPickedUpEvent,
                DeliveryDeliveredEvent, DeliveryCancelledEvent {

    Long orderId();
}
