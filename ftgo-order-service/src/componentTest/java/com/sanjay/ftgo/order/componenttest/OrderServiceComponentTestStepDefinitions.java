package com.sanjay.ftgo.order.componenttest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.After;
import io.cucumber.java.Before;
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

public class OrderServiceComponentTestStepDefinitions {

    private static final String ORDER_SERVICE_BASE_URL = "http://localhost:8082";
    private static final String WIREMOCK_BASE_URL = "http://localhost:8080";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private SagaParticipantStub sagaParticipantStub;
    private Long placedOrderId;

    @Before
    public void setUp() {
        sagaParticipantStub = new SagaParticipantStub(KAFKA_BOOTSTRAP_SERVERS);
    }

    @After
    public void tearDown() {
        sagaParticipantStub.close();
    }

    @Given("the Restaurant Service stub is serving restaurant {int} with menu item {int} priced at {double}")
    public void theRestaurantServiceStubIsServing(int restaurantId, int menuItemId, double price) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(WIREMOCK_BASE_URL + "/restaurants/" + restaurantId))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Restaurant Service stub is not serving restaurant " + restaurantId);
        JsonNode body = objectMapper.readTree(response.body());
        assertEquals(menuItemId, body.get("menuItems").get(0).get("id").asInt());
        assertEquals(price, body.get("menuItems").get(0).get("price").asDouble(), 0.001);
    }

    @Given("the saga participant stub will approve the accounting authorization")
    public void theSagaParticipantStubWillApprove() {
        sagaParticipantStub.setAccountingShouldApprove(true);
    }

    @Given("the saga participant stub will decline the accounting authorization")
    public void theSagaParticipantStubWillDecline() {
        sagaParticipantStub.setAccountingShouldApprove(false);
    }

    @When("a consumer places an order for {int} of menu item {int} from restaurant {int}")
    public void aConsumerPlacesAnOrder(int quantity, int menuItemId, int restaurantId) throws Exception {
        String requestBody = String.format(
                "{\"consumerId\":1,\"restaurantId\":%d,\"lineItems\":[{\"menuItemId\":%d,\"quantity\":%d}]}",
                restaurantId, menuItemId, quantity);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ORDER_SERVICE_BASE_URL + "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode(), "Order creation failed: " + response.body());
        JsonNode body = objectMapper.readTree(response.body());
        placedOrderId = body.get("id").asLong();
        assertEquals("APPROVAL_PENDING", body.get("status").asText());
    }

    @Then("the order is eventually approved")
    public void theOrderIsEventuallyApproved() throws Exception {
        assertEquals("APPROVED", pollForFinalStatus());
    }

    @Then("the order is eventually rejected")
    public void theOrderIsEventuallyRejected() throws Exception {
        assertEquals("REJECTED", pollForFinalStatus());
    }

    private String pollForFinalStatus() throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        String lastStatus = "APPROVAL_PENDING";
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ORDER_SERVICE_BASE_URL + "/orders/" + placedOrderId))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = objectMapper.readTree(response.body());
            lastStatus = body.get("status").asText();
            if (!"APPROVAL_PENDING".equals(lastStatus)) {
                return lastStatus;
            }
            Thread.sleep(500);
        }
        fail("Order " + placedOrderId + " did not leave APPROVAL_PENDING within 10s; last status: " + lastStatus);
        return lastStatus;
    }
}
