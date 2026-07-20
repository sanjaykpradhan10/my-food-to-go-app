package com.sanjay.ftgo.kitchen.domain;

public sealed interface TicketDomainEvent
        permits TicketCreatedEvent, TicketCreationFailedEvent, TicketConfirmedEvent, TicketCancelledEvent,
                TicketAcceptedEvent, TicketPreparingStartedEvent, TicketReadyForPickupEvent, TicketPickedUpEvent {

    Long orderId();
}
