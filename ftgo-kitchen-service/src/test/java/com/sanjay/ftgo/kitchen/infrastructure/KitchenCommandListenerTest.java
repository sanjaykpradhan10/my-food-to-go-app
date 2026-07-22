package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KitchenCommandListenerTest {

    private final TicketService ticketService = mock(TicketService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KitchenCommandListener listener = new KitchenCommandListener(ticketService, objectMapper);

    @Test
    void dispatchesCreateTicketCommand() {
        String payload = """
                {"eventId":"c1","commandType":"CreateTicket","orderId":42,"totalQuantity":2,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleCreateTicketCommand("c1", 42L, 2);
    }

    @Test
    void dispatchesConfirmTicketCommand() {
        String payload = """
                {"eventId":"c2","commandType":"ConfirmTicket","orderId":42,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleConfirmTicketCommand("c2", 42L);
    }

    @Test
    void dispatchesCancelTicketCommand() {
        String payload = """
                {"eventId":"c3","commandType":"CancelTicket","orderId":42,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleCancelTicketCommand("c3", 42L, "CancelOrder");
    }

    @Test
    void dispatchesReviseTicketCommand() {
        String payload = """
                {"eventId":"c10","commandType":"ReviseTicket","orderId":42,"totalQuantity":8,"sagaType":"ReviseOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleReviseTicketCommand("c10", 42L, 8);
    }

    @Test
    void dispatchesUndoReviseTicketCommand() {
        String payload = """
                {"eventId":"c11","commandType":"UndoReviseTicket","orderId":42,"totalQuantity":2,"sagaType":"ReviseOrder"}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleUndoReviseTicketCommand("c11", 42L, 2);
    }
}
