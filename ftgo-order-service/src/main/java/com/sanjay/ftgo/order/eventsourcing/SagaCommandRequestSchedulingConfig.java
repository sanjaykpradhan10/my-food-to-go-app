package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.time.Instant;

// Mirrors ftgo-common's OutboxSchedulingConfig: a plain @Scheduled(fixedDelayString = ...)
// method resolves its placeholder once, at bean-creation time, so it can never observe a later
// config-server refresh. Registering a Trigger here instead means Spring calls
// nextExecution(...) fresh before every run, reading the current value of the
// @RefreshScope-backed SagaCommandRequestProperties bean each time.
@Configuration
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class SagaCommandRequestSchedulingConfig implements SchedulingConfigurer {

    private final SagaCommandRequestPublisher publisher;
    private final SagaCommandRequestProperties properties;

    public SagaCommandRequestSchedulingConfig(SagaCommandRequestPublisher publisher,
                                               SagaCommandRequestProperties properties) {
        this.publisher = publisher;
        this.properties = properties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(publisher::publishPending, new PollTrigger(properties));
    }

    static class PollTrigger implements Trigger {

        private final SagaCommandRequestProperties properties;

        PollTrigger(SagaCommandRequestProperties properties) {
            this.properties = properties;
        }

        @Override
        public Instant nextExecution(TriggerContext triggerContext) {
            Instant lastCompletion = triggerContext.lastCompletion();
            Instant base = (lastCompletion != null) ? lastCompletion : Instant.now();
            return base.plusMillis(properties.getPollFixedDelayMs());
        }
    }
}
