package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

public class OrderDetailsHandler {

    private final BackendClients clients;

    public OrderDetailsHandler(BackendClients clients) {
        this.clients = clients;
    }

    public Mono<OrderDetails> fetchOrderDetails(Long orderId) {
        Mono<SectionResult<String>> order = fetchSection(clients.orderServiceClient(), "/orders/" + orderId);
        Mono<SectionResult<String>> ticket = fetchSection(clients.kitchenServiceClient(), "/tickets/order/" + orderId);
        Mono<SectionResult<String>> authorization = fetchSection(clients.accountingServiceClient(), "/authorizations/order/" + orderId);
        Mono<SectionResult<String>> delivery = fetchSection(clients.deliveryServiceClient(), "/deliveries/order/" + orderId);

        return Mono.zip(order, ticket, authorization, delivery)
                .map(tuple -> new OrderDetails(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4()));
    }

    // onErrorResume distinguishes a 404 (section genuinely absent, e.g. no delivery yet
    // scheduled) from any other failure (timeout, connection refused, 5xx) so the client can
    // tell "not created yet" apart from "backend is down" while still degrading gracefully
    // either way instead of failing the whole composed response.
    private Mono<SectionResult<String>> fetchSection(WebClient client, String path) {
        return client.get()
                .uri(path)
                .retrieve()
                .bodyToMono(String.class)
                .<SectionResult<String>>map(SectionResult.Found::new)
                .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.just(new SectionResult.NotFound<>()))
                .onErrorResume(Exception.class, e -> Mono.just(new SectionResult.Unavailable<>()));
    }
}
