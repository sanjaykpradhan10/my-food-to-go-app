package com.sanjay.ftgo.common.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

// @RefreshScope wraps this bean in a proxy that gets torn down and re-bound to fresh property
// values on every POST /actuator/refresh, so OutboxSchedulingConfig always reads the current
// outbox.poll-fixed-delay-ms — including a value pulled from ftgo-config-server after a live
// config-repo edit, without restarting the process.
@Component
@RefreshScope
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    private long pollFixedDelayMs = 2000;

    public long getPollFixedDelayMs() {
        return pollFixedDelayMs;
    }

    public void setPollFixedDelayMs(long pollFixedDelayMs) {
        this.pollFixedDelayMs = pollFixedDelayMs;
    }
}
