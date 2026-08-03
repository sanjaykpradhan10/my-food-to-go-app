package com.sanjay.ftgo.gateway.common;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple in-memory, per-caller fixed-window rate limiter. Spring Cloud Gateway's built-in
 * RequestRateLimiter requires Redis; this project has no Redis instance, so this trades away
 * multi-instance correctness (each gateway instance counts independently) for zero new
 * infrastructure, acceptable for a single-instance dev/learning deployment.
 *
 * Keys off the JWT 'sub' claim that JwtValidationFilter (a GlobalFilter ordered before any
 * route-specific filter, including this one) stashes on the exchange after validating the
 * caller's bearer token — replaces the Ch.8 X-Api-Key header, which nothing sends anymore now
 * that API-key auth has been retired in favor of OAuth2/JWT.
 */
@Component
public class PerKeyRateLimiterGatewayFilterFactory
        extends AbstractGatewayFilterFactory<PerKeyRateLimiterGatewayFilterFactory.Config> {

    private static final long WINDOW_MILLIS = 1000L;

    private final Map<String, Window> windowsByKey = new ConcurrentHashMap<>();

    public PerKeyRateLimiterGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            // Defensive fallback only: JwtValidationFilter runs first in the filter chain (order
            // Integer.MIN_VALUE + 1) and rejects unauthenticated requests with 401 before they
            // ever reach a route filter, so this attribute should always be present here.
            Jwt jwt = exchange.getAttribute(JwtValidationFilter.VALIDATED_JWT_ATTRIBUTE);
            String effectiveKey = jwt != null ? jwt.getSubject() : "anonymous";
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
