package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

// Distinct from ftgo-common's outbox.poll-fixed-delay-ms: this poller drains the
// order_saga_command_requests table (event-sourcing mode's saga command outbox), a separate
// table from the domain-event outbox that OutboxPublisher services, so it gets its own key rather
// than coincidentally sharing "outbox.poll-fixed-delay-ms" — two independently-pollable
// mechanisms should not be governed by the same property. @RefreshScope mirrors OutboxProperties
// so this interval, too, can be changed live via POST /actuator/refresh.
@Component
@RefreshScope
@ConfigurationProperties(prefix = "saga.command-request")
public class SagaCommandRequestProperties {

    private long pollFixedDelayMs = 2000;

    public long getPollFixedDelayMs() {
        return pollFixedDelayMs;
    }

    public void setPollFixedDelayMs(long pollFixedDelayMs) {
        this.pollFixedDelayMs = pollFixedDelayMs;
    }
}
