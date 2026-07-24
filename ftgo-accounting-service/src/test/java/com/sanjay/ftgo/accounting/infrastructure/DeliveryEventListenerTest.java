package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final SagaJoinService sagaJoinService = mock(SagaJoinService.class);
    private final AuthorizationCancelService authorizationCancelService = mock(AuthorizationCancelService.class);
    private final DeliveryEventListener listener =
            new DeliveryEventListener(sagaJoinService, authorizationCancelService, new ObjectMapper());

    @Test
    void deliveryScheduledFeedsTheJoin() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleDeliveryEvent("evt-1", 42L, "DeliveryScheduled");
        verifyNoInteractions(authorizationCancelService);
    }

    @Test
    void deliverySchedulingFailedFeedsTheJoin() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliverySchedulingFailed","orderId":42,"reason":"no courier available"}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleDeliveryEvent("evt-1", 42L, "DeliverySchedulingFailed");
    }

    @Test
    void deliveryCancelledTriggersReversal() {
        String payload = """
                {"eventId":"evt-2","eventType":"DeliveryCancelled","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verify(authorizationCancelService).reverseForChoreography("evt-2", 42L);
        verifyNoInteractions(sagaJoinService);
    }
}
