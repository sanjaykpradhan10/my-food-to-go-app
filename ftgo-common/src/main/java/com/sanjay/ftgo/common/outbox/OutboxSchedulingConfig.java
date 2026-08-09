package com.sanjay.ftgo.common.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.time.Instant;

// A plain @Scheduled(fixedDelayString = "${outbox.poll-fixed-delay-ms}") method resolves that
// placeholder once, at bean-creation time — it can never observe a later config-server refresh.
// Registering a Trigger here instead means Spring calls nextExecution(...) fresh before every
// run, so it can read the current value of the @RefreshScope-backed OutboxProperties bean each
// time, making the polling interval live-refreshable.
@Configuration
@ConditionalOnProperty(name = "outbox.publish-mode", havingValue = "polling", matchIfMissing = true)
public class OutboxSchedulingConfig implements SchedulingConfigurer {

    private final OutboxPublisher outboxPublisher;
    private final OutboxProperties outboxProperties;

    public OutboxSchedulingConfig(OutboxPublisher outboxPublisher, OutboxProperties outboxProperties) {
        this.outboxPublisher = outboxPublisher;
        this.outboxProperties = outboxProperties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(outboxPublisher::publishPendingEvents, new PollTrigger(outboxProperties));
    }

    static class PollTrigger implements Trigger {

        private final OutboxProperties outboxProperties;

        PollTrigger(OutboxProperties outboxProperties) {
            this.outboxProperties = outboxProperties;
        }

        @Override
        public Instant nextExecution(TriggerContext triggerContext) {
            Instant lastCompletion = triggerContext.lastCompletion();
            Instant base = (lastCompletion != null) ? lastCompletion : Instant.now();
            return base.plusMillis(outboxProperties.getPollFixedDelayMs());
        }
    }
}
