package com.sanjay.ftgo.gateway.common;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple in-memory, per-API-key fixed-window rate limiter. Spring Cloud Gateway's built-in
 * RequestRateLimiter requires Redis; this project has no Redis instance, so this trades away
 * multi-instance correctness (each gateway instance counts independently) for zero new
 * infrastructure, acceptable for a single-instance dev/learning deployment.
 */
@Component
public class PerKeyRateLimiterGatewayFilterFactory
        extends AbstractGatewayFilterFactory<PerKeyRateLimiterGatewayFilterFactory.Config> {

    private static final String API_KEY_HEADER = "X-Api-Key";
    private static final long WINDOW_MILLIS = 1000L;

    private final Map<String, Window> windowsByKey = new ConcurrentHashMap<>();

    public PerKeyRateLimiterGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String key = exchange.getRequest().getHeaders().getFirst(API_KEY_HEADER);
            String effectiveKey = key != null ? key : "anonymous";
            Window window = windowsByKey.computeIfAbsent(effectiveKey, k -> new Window());

            if (window.tryAcquire(config.getRequestsPerSecond())) {
                return chain.filter(exchange);
            }
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return exchange.getResponse().setComplete();
        };
    }

    public static class Config {
        private int requestsPerSecond = 10;

        public int getRequestsPerSecond() {
            return requestsPerSecond;
        }

        public void setRequestsPerSecond(int requestsPerSecond) {
            this.requestsPerSecond = requestsPerSecond;
        }
    }

    private static class Window {
        private volatile long windowStart = System.currentTimeMillis();
        private final AtomicInteger count = new AtomicInteger(0);

        synchronized boolean tryAcquire(int limit) {
            long now = System.currentTimeMillis();
            if (now - windowStart >= WINDOW_MILLIS) {
                windowStart = now;
                count.set(0);
            }
            return count.incrementAndGet() <= limit;
        }
    }
}
