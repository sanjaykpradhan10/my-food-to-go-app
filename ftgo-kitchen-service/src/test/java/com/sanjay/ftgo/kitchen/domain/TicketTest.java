package com.sanjay.ftgo.kitchen.domain;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TicketTest {

    @Test
    void createTicketStartsInCreatePendingAndEmitsTicketCreated() {
        TicketCreationResult result = Ticket.createTicket(42L, 3);

        assertThat(result.ticket().getState()).isEqualTo(TicketState.CREATE_PENDING);
        assertThat(result.ticket().getOrderId()).isEqualTo(42L);
        assertThat(result.events()).containsExactly(new TicketCreatedEvent(42L, 3));
    }

    @Test
    void createCancelledStartsDirectlyInCancelled() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(ticket.getOrderId()).isEqualTo(42L);
    }

    @Test
    void confirmMovesFromCreatePendingToAwaitingAcceptance() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        List<TicketDomainEvent> events = ticket.confirm();

        assertThat(ticket.getState()).isEqualTo(TicketState.AWAITING_ACCEPTANCE);
        assertThat(events).containsExactly(new TicketConfirmedEvent(42L));
    }

    @Test
    void confirmFromWrongStateThrows() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(ticket::confirm).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void acceptMovesFromAwaitingAcceptanceToAccepted() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        ZonedDateTime readyBy = ZonedDateTime.now().plusMinutes(30);

        List<TicketDomainEvent> events = ticket.accept(readyBy);

        assertThat(ticket.getState()).isEqualTo(TicketState.ACCEPTED);
        assertThat(events).containsExactly(new TicketAcceptedEvent(42L, readyBy));
    }

    @Test
    void acceptFromWrongStateThrows() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        assertThatThrownBy(() -> ticket.accept(ZonedDateTime.now()))
                .isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void preparingMovesFromAcceptedToPreparing() {
        Ticket ticket = acceptedTicket();

        List<TicketDomainEvent> events = ticket.preparing();

        assertThat(ticket.getState()).isEqualTo(TicketState.PREPARING);
        assertThat(events).containsExactly(new TicketPreparingStartedEvent(42L));
    }

    @Test
    void preparingFromWrongStateThrows() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        assertThatThrownBy(ticket::preparing).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void readyForPickupMovesFromPreparingToReadyForPickup() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();

        List<TicketDomainEvent> events = ticket.readyForPickup();

        assertThat(ticket.getState()).isEqualTo(TicketState.READY_FOR_PICKUP);
        assertThat(events).containsExactly(new TicketReadyForPickupEvent(42L));
    }

    @Test
    void readyForPickupFromWrongStateThrows() {
        Ticket ticket = acceptedTicket();

        assertThatThrownBy(ticket::readyForPickup).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void pickedUpMovesFromReadyForPickupToPickedUp() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();

        List<TicketDomainEvent> events = ticket.pickedUp();

        assertThat(ticket.getState()).isEqualTo(TicketState.PICKED_UP);
        assertThat(events).containsExactly(new TicketPickedUpEvent(42L));
    }

    @Test
    void pickedUpFromWrongStateThrows() {
        Ticket ticket = acceptedTicket();

        assertThatThrownBy(ticket::pickedUp).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromCreatePendingSucceeds() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        List<TicketDomainEvent> events = ticket.cancel();

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(events).containsExactly(new TicketCancelledEvent(42L));
    }

    @Test
    void cancelFromAwaitingAcceptanceSucceeds() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();

        List<TicketDomainEvent> events = ticket.cancel();

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(events).containsExactly(new TicketCancelledEvent(42L));
    }

    @Test
    void cancelFromAcceptedSucceeds() {
        Ticket ticket = acceptedTicket();

        List<TicketDomainEvent> events = ticket.cancel();

        assertThat(ticket.getState()).isEqualTo(TicketState.CANCELLED);
        assertThat(events).containsExactly(new TicketCancelledEvent(42L));
    }

    @Test
    void cancelFromReadyForPickupThrowsTicketCannotBeCancelled() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();

        assertThatThrownBy(ticket::cancel).isInstanceOf(TicketCannotBeCancelledException.class);
    }

    @Test
    void cancelFromPreparingThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();

        assertThatThrownBy(ticket::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromPickedUpThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();
        ticket.pickedUp();

        assertThatThrownBy(ticket::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromCancelledThrowsUnsupportedStateTransition() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(ticket::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void createTicketStoresTotalQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 7).ticket();

        assertThat(ticket.getTotalQuantity()).isEqualTo(7);
    }

    @Test
    void reviseQuantityFromCreatePendingSucceeds() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        List<TicketDomainEvent> events = ticket.reviseQuantity(8);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        assertThat(events).containsExactly(new TicketQuantityRevisedEvent(42L, 8));
    }

    @Test
    void reviseQuantityFromPreparingSucceeds() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();

        List<TicketDomainEvent> events = ticket.reviseQuantity(8);

        assertThat(ticket.getTotalQuantity()).isEqualTo(8);
        assertThat(events).containsExactly(new TicketQuantityRevisedEvent(42L, 8));
    }

    @Test
    void reviseQuantityFromReadyForPickupThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();

        assertThatThrownBy(() -> ticket.reviseQuantity(8)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseQuantityFromPickedUpThrowsUnsupportedStateTransition() {
        Ticket ticket = acceptedTicket();
        ticket.preparing();
        ticket.readyForPickup();
        ticket.pickedUp();

        assertThatThrownBy(() -> ticket.reviseQuantity(8)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reviseQuantityFromCancelledThrowsUnsupportedStateTransition() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(() -> ticket.reviseQuantity(8)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void undoRevisionRestoresOriginalQuantity() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.reviseQuantity(8);

        List<TicketDomainEvent> events = ticket.undoRevision(3);

        assertThat(ticket.getTotalQuantity()).isEqualTo(3);
        assertThat(events).containsExactly(new TicketRevisionUndoneEvent(42L, 3));
    }

    @Test
    void undoRevisionFromCancelledThrowsUnsupportedStateTransition() {
        Ticket ticket = Ticket.createCancelled(42L);

        assertThatThrownBy(() -> ticket.undoRevision(3)).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    private Ticket acceptedTicket() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ticket.confirm();
        ticket.accept(ZonedDateTime.now().plusMinutes(30));
        return ticket;
    }
}
