package com.sanjay.ftgo.kitchen.domain;

public sealed interface TicketDomainEvent
        permits TicketCreatedEvent, TicketCreationFailedEvent, TicketConfirmedEvent, TicketCancelledEvent,
                TicketCancellationRejectedEvent, TicketAcceptedEvent, TicketPreparingStartedEvent,
                TicketReadyForPickupEvent, TicketPickedUpEvent, TicketQuantityRevisedEvent,
                TicketRevisionRejectedEvent, TicketRevisionUndoneEvent {

    Long orderId();
}
