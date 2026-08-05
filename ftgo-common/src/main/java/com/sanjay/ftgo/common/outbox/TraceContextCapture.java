package com.sanjay.ftgo.common.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Captures the trace context active when an {@link OutboxEvent} is created, so
 * {@link OutboxPublisher}'s later, disconnected {@code @Scheduled} poll can re-parent its Kafka
 * send under the original request/message's trace instead of starting a brand-new one.
 * <p>
 * {@code OutboxEvent} is a plain JPA entity with no DI, and it's created from ~10 call sites
 * spread across every Kafka-producing service's domain code — threading a {@link Propagator}
 * through all of them just for this would be far more invasive than capturing it here, once, via
 * a static reference populated at startup. {@link Tracer}/{@link Propagator} are optional
 * ({@code required = false}): a plain unit test or a service with no tracing infrastructure
 * configured leaves this null, and callers must tolerate a null/empty traceparent (same as before
 * this feature existed — an untraced Kafka send, not a startup failure).
 */
@Component
public class TraceContextCapture {

    private static volatile Tracer tracer;
    private static volatile Propagator propagator;

    @Autowired(required = false)
    public TraceContextCapture(@Nullable Tracer tracer, @Nullable Propagator propagator) {
        TraceContextCapture.tracer = tracer;
        TraceContextCapture.propagator = propagator;
    }

    /**
     * @return the W3C {@code traceparent} value for the currently active span, or {@code null} if
     * there's no active span or no tracing infrastructure configured.
     */
    static String captureCurrentTraceparent() {
        Tracer t = tracer;
        Propagator p = propagator;
        if (t == null || p == null) {
            return null;
        }
        Span currentSpan = t.currentSpan();
        if (currentSpan == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        p.inject(currentSpan.context(), carrier, Map::put);
        return carrier.get("traceparent");
    }
}
