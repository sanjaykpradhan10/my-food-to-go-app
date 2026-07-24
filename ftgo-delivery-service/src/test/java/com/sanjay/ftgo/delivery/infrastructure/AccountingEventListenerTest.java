package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class AccountingEventListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final AccountingEventListener listener = new AccountingEventListener(deliveryService, new ObjectMapper());

    @Test
    void onCardAuthorizationFailedReleasesDelivery() {
        String payload = """
                {"eventId":"evt-1","eventType":"CardAuthorizationFailed","orderId":42}
                """;

        listener.onMessage(payload);

        verify(deliveryService).release("evt-1", 42L);
    }

    @Test
    void ignoresCardAuthorized() {
        String payload = """
                {"eventId":"evt-1","eventType":"CardAuthorized","orderId":42}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(deliveryService);
    }
}
