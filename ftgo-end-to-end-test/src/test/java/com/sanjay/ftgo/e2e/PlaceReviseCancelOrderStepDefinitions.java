package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class PlaceReviseCancelOrderStepDefinitions {

    private static final String RESTAURANT_SERVICE_BASE_URL = "http://localhost:8085";
    private static final String CONSUMER_SERVICE_BASE_URL = "http://localhost:8081";
    private static final String GATEWAY_BASE_URL = "http://localhost:8091/api/v1";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TokenClient tokenClient = new TokenClient();
    private final OrderIdHolder orderIdHolder;

    private long restaurantId;
    private long menuItemId;
    private long consumerId;
    private long orderId;

    public PlaceReviseCancelOrderStepDefinitions(OrderIdHolder orderIdHolder) {
        this.orderIdHolder = orderIdHolder;
    }

    @Given("a restaurant {string} with a menu item {string} priced at {double}")
    public void aRestaurantWithAMenuItemPricedAt(String restaurantName, String menuItemName, double price) throws Exception {
        String body = String.format(
                "{\"name\":\"%s\",\"menuItems\":[{\"name\":\"%s\",\"price\":%s}]}",
                restaurantName, menuItemName, price);
        // Direct (non-gateway) call to restaurant-service, which now requires a RESTAURANT or
        // ADMIN token per Task 4's @PreAuthorize on POST /restaurants.
        JsonNode response = postWithRetry(RESTAURANT_SERVICE_BASE_URL + "/restaurants", body,
                "Bearer " + tokenClient.tokenFor("restaurant1", "password"));
        restaurantId = response.get("id").asLong();
        menuItemId = response.get("menuItems").get(0).get("id").asLong();
    }

    @Given("an active consumer {string}")
    public void anActiveConsumer(String consumerName) throws Exception {
        String body = String.format("{\"name\":\"%s\",\"active\":true}", consumerName);
        // Direct (non-gateway) call to consumer-service, which now requires an ADMIN token per
        // Task 4's @PreAuthorize on POST /consumers.
        JsonNode response = postWithRetry(CONSUMER_SERVICE_BASE_URL + "/consumers", body,
                "Bearer " + tokenClient.tokenFor("admin1", "password"));
        consumerId = response.get("id").asLong();
    }

    @When("the consumer places an order for {int} of the menu item at the restaurant")
    public void theConsumerPlacesAnOrder(int quantity) throws Exception {
        String body = String.format(
                "{\"consumerId\":%d,\"restaurantId\":%d,\"lineItems\":[{\"menuItemId\":%d,\"quantity\":%d}]}",
                consumerId, restaurantId, menuItemId, quantity);
        // Order-service and the gateway's route to it may still be registering with Eureka even
        // after restaurant/consumer setup succeeded, so this call gets the same retry treatment.
        JsonNode response = postWithRetry(GATEWAY_BASE_URL + "/orders", body,
                "Bearer " + tokenClient.tokenFor("consumer1", "password"));
        orderId = response.get("id").asLong();
        orderIdHolder.set(orderId);
    }

    @When("the consumer revises the order to {int} of the menu item")
    public void theConsumerRevisesTheOrder(int quantity) throws Exception {
        String body = String.format("{\"lineItems\":[{\"menuItemId\":%d,\"quantity\":%d}]}", menuItemId, quantity);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GATEWAY_BASE_URL + "/orders/" + orderId + "/revise"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + tokenClient.tokenFor("consumer1", "password"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Order revision failed: " + response.body());
    }

    @When("the consumer cancels the order")
    public void theConsumerCancelsTheOrder() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GATEWAY_BASE_URL + "/orders/" + orderId + "/cancel"))
                .header("Authorization", "Bearer " + tokenClient.tokenFor("consumer1", "password"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Order cancellation failed: " + response.body());
    }

    @Then("the order is eventually approved")
    public void theOrderIsEventuallyApproved() throws Exception {
        assertEquals("APPROVED", pollForFinalStatus("APPROVAL_PENDING"));
    }

    @Then("the revision is eventually declined and the order keeps its original quantity of {int}")
    public void theRevisionIsEventuallyDeclined(int originalQuantity) throws Exception {
        // A declined revision returns status to APPROVED (Order.rejectRevision(), not REJECTED
        // — that status is reserved for a declined initial authorization), so status alone can't
        // distinguish "revision declined" from "revision never attempted". Assert on the
        // line-item quantity reverting instead, once status has left REVISION_PENDING.
        assertEquals("APPROVED", pollForFinalStatus("REVISION_PENDING"));
        assertEquals(originalQuantity, fetchOrder().get("lineItems").get(0).get("quantity").asInt());
    }

    @Then("the order is eventually cancelled")
    public void theOrderIsEventuallyCancelled() throws Exception {
        assertEquals("CANCELLED", pollForFinalStatus("CANCEL_PENDING"));
    }

    private JsonNode fetchOrder() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GATEWAY_BASE_URL + "/orders/" + orderId))
                .header("Authorization", "Bearer " + tokenClient.tokenFor("consumer1", "password"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body());
    }

    // Every setup/creation call runs before the gateway's routed services have necessarily
    // finished registering with Eureka after container startup (this includes order-service
    // itself, reached via the gateway) — retry with backoff instead of requiring a separate
    // explicit readiness wait in the compose config.
    private JsonNode postWithRetry(String url, String body, String authorizationHeaderValue) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .header("Authorization", authorizationHeaderValue);
                HttpRequest request = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body)).build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 201) {
                    return objectMapper.readTree(response.body());
                }
                lastFailure = new IllegalStateException("Unexpected status " + response.statusCode() + ": " + response.body());
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(2000);
        }
        throw new IllegalStateException("Failed to POST to " + url + " within 60s", lastFailure);
    }

    private String pollForFinalStatus(String pendingStatus) throws Exception {
        // 30s: mirrors sub-project 2's component test budget for outbox-poller latency
        // (fixed-delay 2000ms) plus Kafka delivery/consumer lag across the saga's participants.
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        String lastStatus = pendingStatus;
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(GATEWAY_BASE_URL + "/orders/" + orderId))
                    .header("Authorization", "Bearer " + tokenClient.tokenFor("consumer1", "password"))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = objectMapper.readTree(response.body());
            lastStatus = body.get("status").asText();
            if (!pendingStatus.equals(lastStatus)) {
                return lastStatus;
            }
            Thread.sleep(500);
        }
        fail("Order " + orderId + " did not leave " + pendingStatus + " within 30s; last status: " + lastStatus);
        return lastStatus;
    }
}
