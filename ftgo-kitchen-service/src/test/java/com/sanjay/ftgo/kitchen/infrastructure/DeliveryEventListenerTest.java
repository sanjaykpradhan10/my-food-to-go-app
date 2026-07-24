package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final TicketService ticketService = mock(TicketService.class);
    private final DeliveryEventListener listener = new DeliveryEventListener(ticketService, new ObjectMapper());

    @Test
    void deliverySchedulingFailedCancelsTicket() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliverySchedulingFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(ticketService).handleConsumerVerificationFailed("evt-1", 42L);
    }

    @Test
    void ignoresDeliveryScheduled() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(ticketService);
    }
}
