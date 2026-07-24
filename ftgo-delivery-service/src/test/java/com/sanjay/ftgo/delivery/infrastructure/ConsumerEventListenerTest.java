package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ConsumerEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final ConsumerEventListener listener = new ConsumerEventListener(deliveryService, new ObjectMapper());

    @Test
    void onConsumerVerificationFailedReleasesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"ConsumerVerificationFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-1", 42L);
    }

    @Test
    void ignoresConsumerVerified() {
        String payload = """
                {"eventId":"evt-1","eventType":"ConsumerVerified","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }
}
