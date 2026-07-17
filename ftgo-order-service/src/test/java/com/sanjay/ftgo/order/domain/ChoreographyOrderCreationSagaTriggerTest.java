package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ChoreographyOrderCreationSagaTriggerTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ChoreographyOrderCreationSagaTrigger trigger =
            new ChoreographyOrderCreationSagaTrigger(outboxEventRepository, objectMapper);

    @Test
    void writesOrderCreatedToOrderEventsTopic() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

        trigger.onOrderCreated(order, "event-1");

        verify(outboxEventRepository).save(argThat(e ->
                "OrderCreated".equals(e.getEventType())
                        && "order.events".equals(e.getTopic())
                        && e.getPayload().contains("\"restaurantId\":1")));
    }
}
