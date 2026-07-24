package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final OrderEventListener listener = new OrderEventListener(deliveryService, new ObjectMapper());

    @Test
    void onOrderCreatedSchedulesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"OrderCreated","orderId":42,"restaurantId":7}
                """;

        listener.onMessage(payload);

        verify(deliveryService).handleOrderCreated("evt-1", 42L, 7L);
    }

    @Test
    void ignoresOtherEventTypes() {
        String payload = """
                {"eventId":"evt-1","eventType":"OrderCancelled","orderId":42,"restaurantId":7}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(deliveryService);
    }
}
