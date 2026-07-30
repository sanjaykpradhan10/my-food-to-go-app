package com.sanjay.ftgo.gateway.common;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Stub authentication edge function: this project has no real identity service, so this
 * checks a single shared-secret header rather than a token. Real auth is out of scope for
 * Ch.8 — see the design spec's non-goals.
 */
@Component
public class ApiKeyAuthFilter implements GlobalFilter, Ordered {

    private static final String API_KEY_HEADER = "X-Api-Key";

    private final GatewayApiKeyProperties properties;

    public ApiKeyAuthFilter(GatewayApiKeyProperties properties) {
        this.properties = properties;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String providedKey = exchange.getRequest().getHeaders().getFirst(API_KEY_HEADER);
        if (providedKey == null || !providedKey.equals(properties.value())) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE + 1;
    }
}
