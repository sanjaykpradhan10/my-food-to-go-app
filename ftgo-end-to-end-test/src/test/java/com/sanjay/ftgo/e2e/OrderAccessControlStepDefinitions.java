package com.sanjay.ftgo.e2e;

import io.cucumber.java.en.Then;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderAccessControlStepDefinitions {

    private static final String GATEWAY_BASE_URL = "http://localhost:8091/api/v1";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final TokenClient tokenClient = new TokenClient();
    private final OrderIdHolder orderIdHolder;

    public OrderAccessControlStepDefinitions(OrderIdHolder orderIdHolder) {
        this.orderIdHolder = orderIdHolder;
    }

    @Then("the consumer can fetch their own order")
    public void theConsumerCanFetchTheirOwnOrder() throws Exception {
        HttpResponse<String> response = fetchOrder("Bearer " + tokenClient.tokenFor("consumer1", "password"));
        assertEquals(200, response.statusCode());
    }

    @Then("a different consumer is forbidden from fetching the order")
    public void aDifferentConsumerIsForbidden() throws Exception {
        HttpResponse<String> response = fetchOrder("Bearer " + tokenClient.tokenFor("consumer2", "password"));
        assertEquals(403, response.statusCode());
    }

    @Then("fetching the order with no Authorization header returns 401")
    public void noAuthorizationHeaderReturns401() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GATEWAY_BASE_URL + "/orders/" + orderIdHolder.get()))
                .GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(401, response.statusCode());
    }

    @Then("fetching the order with a malformed token returns 401")
    public void malformedTokenReturns401() throws Exception {
        HttpResponse<String> response = fetchOrder("Bearer not-a-real-jwt");
        assertEquals(401, response.statusCode());
    }

    private HttpResponse<String> fetchOrder(String authorizationHeaderValue) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GATEWAY_BASE_URL + "/orders/" + orderIdHolder.get()))
                .header("Authorization", authorizationHeaderValue)
                .GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
