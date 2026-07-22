package com.sanjay.ftgo.order.eventsourcing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SagaCommandRequestPublisherTest {

    @Mock
    private OrderSagaCommandRequestRepository repository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private SagaCommandRequestPublisher publisher;

    @Test
    void publishesPendingRequestsAndMarksThemPublished() {
        publisher = new SagaCommandRequestPublisher(repository, kafkaTemplate);
        OrderSagaCommandRequest request =
                new OrderSagaCommandRequest("evt-1", "CreateTicket", 42L, "kitchen.commands", "{\"foo\":1}");
        when(repository.findByPublishedAtIsNullOrderByIdAsc()).thenReturn(List.of(request));

        publisher.publishPending();

        verify(kafkaTemplate).send("kitchen.commands", "42", "{\"foo\":1}");
        verify(repository).save(request);
        assertThat(request.isPublished()).isTrue();
    }
}
