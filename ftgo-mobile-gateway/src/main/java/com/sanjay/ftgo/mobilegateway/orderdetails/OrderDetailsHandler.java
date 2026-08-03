package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreakerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class OrderDetailsHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderDetailsHandler.class);

    // Kept short relative to the 5s wait-duration-in-open-state configured for these instances
    // in application.yml, so a hung backend degrades this one section well before the circuit
    // breaker's own failure-rate window would trip open on it.
    private static final Duration SECTION_TIMEOUT = Duration.ofSeconds(2);

    private final BackendClients clients;
    private final ReactiveCircuitBreakerFactory circuitBreakerFactory;

    public OrderDetailsHandler(BackendClients clients, ReactiveCircuitBreakerFactory circuitBreakerFactory) {
        this.clients = clients;
        this.circuitBreakerFactory = circuitBreakerFactory;
    }

    public Mono<OrderDetails> fetchOrderDetails(Long orderId, String bearerToken) {
        Mono<SectionResult<String>> order = fetchSection(
                "orderService", clients.orderServiceClient(), "/orders/" + orderId, bearerToken);
        Mono<SectionResult<String>> ticket = fetchSection(
                "kitchenService", clients.kitchenServiceClient(), "/tickets/order/" + orderId, bearerToken);
        Mono<SectionResult<String>> authorization = fetchSection(
                "accountingService", clients.accountingServiceClient(), "/authorizations/order/" + orderId, bearerToken);
        Mono<SectionResult<String>> delivery = fetchSection(
                "deliveryService", clients.deliveryServiceClient(), "/deliveries/order/" + orderId, bearerToken);

        return Mono.zip(order, ticket, authorization, delivery)
                .map(tuple -> new OrderDetails(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4()));
    }

    // Each backend call is wrapped in the reactive circuit breaker matching the instance name
    // already configured in application.yml (Task 6), so repeated failures against one backend
    // trip that backend's breaker independently of the others instead of hammering a service
    // that's already down. onErrorResume still distinguishes a 404 (section genuinely absent,
    // e.g. no delivery yet scheduled) from any other failure (timeout, connection refused, 5xx,
    // open circuit) so the client can tell "not created yet" apart from "backend unavailable"
    // while still degrading gracefully either way instead of failing the whole composed response.
    private Mono<SectionResult<String>> fetchSection(String circuitBreakerName, WebClient client, String path, String bearerToken) {
        ReactiveCircuitBreaker circuitBreaker = circuitBreakerFactory.create(circuitBreakerName);

        // Forward the caller's own validated token rather than a separate service identity, so
        // each backend's instance-based ACL (e.g. order-service's per-consumer ACL) still applies
        // to the actual requesting user instead of being bypassed by a gateway-wide credential.
        Mono<SectionResult<String>> call = client.get()
                .uri(path)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(SECTION_TIMEOUT)
                .<SectionResult<String>>map(SectionResult.Found::new)
                .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.just(new SectionResult.NotFound<>()));

        return circuitBreaker.run(call, throwable -> {
            log.warn("Section '{}' unavailable for path {} (falling back to UNAVAILABLE): {}",
                    circuitBreakerName, path, throwable.toString());
            return Mono.just(new SectionResult.Unavailable<>());
        });
    }
}
