package com.sanjay.ftgo.common.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TriggerContext;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxSchedulingConfigTest {

    @Test
    void nextExecutionUsesCurrentPropertyValue() {
        OutboxProperties properties = new OutboxProperties();
        properties.setPollFixedDelayMs(2000);
        OutboxSchedulingConfig.PollTrigger trigger = new OutboxSchedulingConfig.PollTrigger(properties);

        Instant lastCompletion = Instant.parse("2026-01-01T00:00:00Z");
        TriggerContext context = mock(TriggerContext.class);
        when(context.lastCompletion()).thenReturn(lastCompletion);

        Instant firstNext = trigger.nextExecution(context);
        assertThat(firstNext).isEqualTo(lastCompletion.plusMillis(2000));

        // Simulate a live refresh changing the bound property value.
        properties.setPollFixedDelayMs(500);
        Instant secondNext = trigger.nextExecution(context);
        assertThat(secondNext).isEqualTo(lastCompletion.plusMillis(500));
    }

    @Test
    void fallsBackToNowWhenNoPriorCompletion() {
        OutboxProperties properties = new OutboxProperties();
        properties.setPollFixedDelayMs(1000);
        OutboxSchedulingConfig.PollTrigger trigger = new OutboxSchedulingConfig.PollTrigger(properties);

        TriggerContext context = mock(TriggerContext.class);
        when(context.lastCompletion()).thenReturn(null);

        Instant before = Instant.now();
        Instant next = trigger.nextExecution(context);
        Instant after = Instant.now();

        assertThat(next).isBetween(before.plusMillis(1000), after.plusMillis(1000));
    }
}
