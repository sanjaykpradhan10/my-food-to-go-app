package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.springframework.web.reactive.function.client.WebClient;

public record BackendClients(
        WebClient orderServiceClient,
        WebClient kitchenServiceClient,
        WebClient accountingServiceClient,
        WebClient deliveryServiceClient) {
}
