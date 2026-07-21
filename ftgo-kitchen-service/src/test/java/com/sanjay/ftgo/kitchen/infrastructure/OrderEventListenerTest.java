package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.OrderCreatedEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OrderEventListenerTest {

    private final TicketService ticketService = mock(TicketService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OrderEventListener listener = new OrderEventListener(ticketService, objectMapper);

    @Test
    void handlesOrderCreatedEvent() {
        String payload = """
                {"eventId":"e1","eventType":"OrderCreated","orderId":42,"restaurantId":1,
                 "lineItems":[{"menuItemId":10,"quantity":2}]}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleOrderCreated(any(OrderCreatedEvent.class));
    }

    @Test
    void ignoresNonOrderCreatedEventTypesInsteadOfMisfiringOnNullFields() {
        String payload = """
                {"eventId":"e2","eventType":"OrderApproved","orderId":42}
                """;

        listener.onMessage(payload);

        verify(ticketService, never()).handleOrderCreated(any());
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verify(ticketService, never()).handleOrderCreated(any());
    }

    @Test
    void handlesOrderCancelledEvent() {
        String payload = """
                {"eventId":"e3","eventType":"OrderCancelled","orderId":42}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleOrderCancelled("e3", 42L);
    }
}
