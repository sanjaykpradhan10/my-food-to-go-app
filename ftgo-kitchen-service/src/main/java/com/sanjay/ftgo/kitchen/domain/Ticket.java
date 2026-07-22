package com.sanjay.ftgo.kitchen.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.ZonedDateTime;
import java.util.List;

@Entity
@Table(name = "tickets")
public class Ticket {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    @Enumerated(EnumType.STRING)
    private TicketState state;

    private ZonedDateTime readyBy;

    private int totalQuantity;

    protected Ticket() {
    }

    private Ticket(Long orderId, TicketState state) {
        this.orderId = orderId;
        this.state = state;
    }

    public static TicketCreationResult createTicket(Long orderId, int totalQuantity) {
        Ticket ticket = new Ticket(orderId, TicketState.CREATE_PENDING);
        ticket.totalQuantity = totalQuantity;
        return new TicketCreationResult(ticket, List.of(new TicketCreatedEvent(orderId, totalQuantity)));
    }

    public static Ticket createCancelled(Long orderId) {
        return new Ticket(orderId, TicketState.CANCELLED);
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public TicketState getState() {
        return state;
    }

    public int getTotalQuantity() {
        return totalQuantity;
    }

    public List<TicketDomainEvent> confirm() {
        if (state != TicketState.CREATE_PENDING) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.AWAITING_ACCEPTANCE;
        return List.of(new TicketConfirmedEvent(orderId));
    }

    public List<TicketDomainEvent> accept(ZonedDateTime readyBy) {
        if (state != TicketState.AWAITING_ACCEPTANCE) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.ACCEPTED;
        this.readyBy = readyBy;
        return List.of(new TicketAcceptedEvent(orderId, readyBy));
    }

    public List<TicketDomainEvent> preparing() {
        if (state != TicketState.ACCEPTED) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.PREPARING;
        return List.of(new TicketPreparingStartedEvent(orderId));
    }

    public List<TicketDomainEvent> readyForPickup() {
        if (state != TicketState.PREPARING) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.READY_FOR_PICKUP;
        return List.of(new TicketReadyForPickupEvent(orderId));
    }

    public List<TicketDomainEvent> pickedUp() {
        if (state != TicketState.READY_FOR_PICKUP) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.state = TicketState.PICKED_UP;
        return List.of(new TicketPickedUpEvent(orderId));
    }

    public List<TicketDomainEvent> cancel() {
        return switch (state) {
            case CREATE_PENDING, AWAITING_ACCEPTANCE, ACCEPTED -> {
                this.state = TicketState.CANCELLED;
                yield List.of(new TicketCancelledEvent(orderId));
            }
            case READY_FOR_PICKUP -> throw new TicketCannotBeCancelledException(orderId);
            case PREPARING, PICKED_UP, CANCELLED -> throw new UnsupportedStateTransitionException(state);
        };
    }

    // No two-tier exception split here, unlike cancel() - that distinction is specific to the
    // book's cancel example, not a pattern every guarded transition needs.
    public List<TicketDomainEvent> reviseQuantity(int newTotalQuantity) {
        return switch (state) {
            case CREATE_PENDING, AWAITING_ACCEPTANCE, ACCEPTED, PREPARING -> {
                this.totalQuantity = newTotalQuantity;
                yield List.of(new TicketQuantityRevisedEvent(orderId, newTotalQuantity));
            }
            case READY_FOR_PICKUP, PICKED_UP, CANCELLED -> throw new UnsupportedStateTransitionException(state);
        };
    }

    // No state restriction beyond CANCELLED - this undoes a change this same saga just made,
    // so reverting to a previously-valid quantity is always legal from any other state.
    public List<TicketDomainEvent> undoRevision(int originalTotalQuantity) {
        if (state == TicketState.CANCELLED) {
            throw new UnsupportedStateTransitionException(state);
        }
        this.totalQuantity = originalTotalQuantity;
        return List.of(new TicketRevisionUndoneEvent(orderId, originalTotalQuantity));
    }
}
