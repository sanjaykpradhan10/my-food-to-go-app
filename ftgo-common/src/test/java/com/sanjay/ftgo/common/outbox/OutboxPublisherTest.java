package com.sanjay.ftgo.common.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class OutboxPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final OutboxPublisher outboxPublisher =
            new OutboxPublisher(outboxEventRepository, kafkaTemplate, 20);

    @Test
    void marksEventSentAfterSuccessfulPublish() {
        OutboxEvent event = new OutboxEvent("event-1", "SomethingHappened", 100L, "some.topic", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("some.topic"), eq("100"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isTrue();
        verify(outboxEventRepository).save(event);
    }

    @Test
    void leavesEventUnsentWhenPublishFails() {
        OutboxEvent event = new OutboxEvent("event-2", "SomethingHappened", 200L, "some.topic", "{}");
        when(outboxEventRepository.findBySentAtIsNullOrderByIdAsc()).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(eq("some.topic"), eq("200"), eq("{}"))).thenReturn(failed);

        outboxPublisher.publishPendingEvents();

        assertThat(event.isSent()).isFalse();
        verify(outboxEventRepository, never()).save(any());
    }
}
