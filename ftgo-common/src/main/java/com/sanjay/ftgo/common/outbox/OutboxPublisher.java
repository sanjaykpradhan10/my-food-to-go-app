package com.sanjay.ftgo.common.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${outbox.batch-size:20}") int batchSize) {
        this(outboxEventRepository, kafkaTemplate, batchSize, null, null);
    }

    @Autowired(required = false)
    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${outbox.batch-size:20}") int batchSize,
                            @Nullable Tracer tracer,
                            @Nullable Propagator propagator) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
        this.tracer = tracer;
        this.propagator = propagator;
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
                sendWithOriginalTraceContext(event);
                event.markSent();
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}, will retry next poll", event.getEventId(), e);
            }
        }
    }

    // This poll runs on its own @Scheduled thread with no trace context of its own, so a plain
    // kafkaTemplate.send() here would always start a brand-new trace rather than continuing the
    // one that actually created the event — defeating the whole point of enabling Kafka producer
    // observation (see KafkaProducerConfig). Re-extracting the traceparent captured at the
    // event's creation time (TraceContextCapture) and making it the current span for the duration
    // of the send lets the Kafka observation instrumentation parent its producer span correctly,
    // so the header it injects on the record actually links back to the original request/message.
    private void sendWithOriginalTraceContext(OutboxEvent event) throws Exception {
        String traceparent = event.getTraceparent();
        if (tracer == null || propagator == null || traceparent == null) {
            kafkaTemplate.send(event.getTopic(), String.valueOf(event.getAggregateId()), event.getPayload()).get();
            return;
        }
        Span.Builder spanBuilder = propagator.extract(Map.of("traceparent", traceparent), Map::get);
        Span span = spanBuilder.name("outbox-publisher.send").kind(Span.Kind.PRODUCER).start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            kafkaTemplate.send(event.getTopic(), String.valueOf(event.getAggregateId()), event.getPayload()).get();
        } finally {
            span.end();
        }
    }
}
