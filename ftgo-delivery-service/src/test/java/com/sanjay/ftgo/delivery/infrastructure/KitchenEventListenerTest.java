package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class KitchenEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final KitchenEventListener listener = new KitchenEventListener(deliveryService, new ObjectMapper());

    @Test
    void onTicketCreationFailedReleasesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"TicketCreationFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-1", 42L);
    }

    @Test
    void onTicketCancelledReleasesDelivery() {
        String payload = """
                {"eventId":"evt-2","eventType":"TicketCancelled","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-2", 42L);
    }

    @Test
    void ignoresTicketCreated() {
        String payload = """
                {"eventId":"evt-1","eventType":"TicketCreated","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }
}
