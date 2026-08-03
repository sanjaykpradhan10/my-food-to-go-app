package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.Then;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class HealthCheckStepDefinitions {

    // Every DB-backed business service except consumer-service; each must report a "db"
    // component UP, in addition to the "discoveryComposite" (Eureka) component. There is no
    // "kafka" component to assert on: Spring Boot's actuator-autoconfigure no longer ships a
    // Kafka health contributor (verified absent from spring-boot-actuator-autoconfigure 3.5.16
    // -- only KafkaMetricsAutoConfiguration remains under actuate.autoconfigure.kafka), and none
    // of the 9 services registers a custom one, so live health JSON never has a "kafka" key.
    private static final List<Map.Entry<String, Integer>> DB_BACKED_SERVICES = List.of(
            Map.entry("order-service", 8082),
            Map.entry("kitchen-service", 8083),
            Map.entry("restaurant-service", 8085),
            Map.entry("accounting-service", 8084),
            Map.entry("delivery-service", 8086),
            Map.entry("order-history-service", 8088)
    );

    // consumer-service is DB-backed but, unlike the other 6, has no eureka-client dependency at
    // all (pre-existing, unrelated to Ch.11) -- it never registers with Eureka, so it has no
    // "discoveryComposite" component either.
    private static final Map.Entry<String, Integer> CONSUMER_SERVICE = Map.entry("consumer-service", 8081);

    // Gateways: no DB/Kafka of their own, but still register with Eureka.
    private static final List<Map.Entry<String, Integer>> GATEWAY_SERVICES = List.of(
            Map.entry("mobile-gateway", 8090),
            Map.entry("public-gateway", 8091)
    );

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Then("every service's health endpoint eventually reports UP")
    public void everyServicesHealthEndpointEventuallyReportsUp() throws Exception {
        for (Map.Entry<String, Integer> service : DB_BACKED_SERVICES) {
            JsonNode health = fetchHealthWithRetry(service.getKey(), service.getValue());
            assertEquals("UP", health.get("status").asText(), service.getKey() + " overall status");
            assertEquals("UP", health.get("components").get("db").get("status").asText(), service.getKey() + " db component");
            assertEquals("UP", health.get("components").get("discoveryComposite").get("status").asText(), service.getKey() + " discoveryComposite component");
        }
        {
            JsonNode health = fetchHealthWithRetry(CONSUMER_SERVICE.getKey(), CONSUMER_SERVICE.getValue());
            assertEquals("UP", health.get("status").asText(), CONSUMER_SERVICE.getKey() + " overall status");
            assertEquals("UP", health.get("components").get("db").get("status").asText(), CONSUMER_SERVICE.getKey() + " db component");
        }
        for (Map.Entry<String, Integer> service : GATEWAY_SERVICES) {
            JsonNode health = fetchHealthWithRetry(service.getKey(), service.getValue());
            assertEquals("UP", health.get("status").asText(), service.getKey() + " overall status");
            assertEquals("UP", health.get("components").get("discoveryComposite").get("status").asText(), service.getKey() + " discoveryComposite component");
        }
    }

    // Mirrors PlaceReviseCancelOrderStepDefinitions's retry-with-backoff pattern: services may
    // still be finishing Eureka self-registration or their first successful DB/Kafka connection
    // check immediately after container startup.
    private JsonNode fetchHealthWithRetry(String serviceName, int port) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/actuator/health"))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                JsonNode body = objectMapper.readTree(response.body());
                if (response.statusCode() == 200 && "UP".equals(body.path("status").asText())) {
                    return body;
                }
                lastFailure = new IllegalStateException(serviceName + " returned " + response.statusCode() + ": " + response.body());
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(2000);
        }
        fail("Health check for " + serviceName + " on port " + port + " did not report UP within 60s", lastFailure);
        return null;
    }
}
