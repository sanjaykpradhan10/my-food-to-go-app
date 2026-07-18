package com.sanjay.ftgo.common.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${outbox.batch-size:20}") int batchSize) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms:2000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findBySentAtIsNullOrderByIdAsc()
                .stream()
                .limit(batchSize)
                .toList();

        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(event.getTopic(), String.valueOf(event.getAggregateId()), event.getPayload()).get();
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }
}
