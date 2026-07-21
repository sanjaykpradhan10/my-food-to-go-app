package com.sanjay.ftgo.consumer.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.consumer.domain.ConsumerVerificationService;
import com.sanjay.ftgo.consumer.domain.OrderCreatedEvent;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OrderEventListenerTest {

    private final ConsumerVerificationService consumerVerificationService = mock(ConsumerVerificationService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OrderEventListener listener = new OrderEventListener(consumerVerificationService, objectMapper);

    @Test
    void handlesOrderCreatedEvent() {
        String payload = """
                {"eventId":"e1","eventType":"OrderCreated","orderId":42,"consumerId":7}
                """;

        listener.onMessage(payload);

        verify(consumerVerificationService).handleOrderCreated(any(OrderCreatedEvent.class));
    }

    @Test
    void ignoresNonOrderCreatedEventTypesInsteadOfMisfiringOnNullFields() {
        String payload = """
                {"eventId":"e2","eventType":"OrderApproved","orderId":42}
                """;

        listener.onMessage(payload);

        verify(consumerVerificationService, never()).handleOrderCreated(any());
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verify(consumerVerificationService, never()).handleOrderCreated(any());
    }
}
