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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuditLoggingStepDefinitions {

    private static final String AUDIT_LOG_SERVICE_BASE_URL = "http://localhost:8089";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TokenClient tokenClient = new TokenClient();
    private final OrderIdHolder orderIdHolder;

    public AuditLoggingStepDefinitions(OrderIdHolder orderIdHolder) {
        this.orderIdHolder = orderIdHolder;
    }

    @Then("the audit log eventually has an entry for the placed order with action containing {string}")
    public void theAuditLogEventuallyHasAnEntry(String actionFragment) throws Exception {
        long orderId = orderIdHolder.get();
        // AuditLoggingAspect (ftgo-common) only fills entityId from a controller method's
        // @PathVariable, and POST /orders (createOrder) has none - the order id doesn't exist
        // until after the call completes. So the createOrder audit entry always has a null
        // entityId, which means entityType=Order&entityId=<id> (the shape used by
        // cancel/revise's audit entries, which DO have a @PathVariable id) can never match this
        // one. AuditLogController only supports single-field lookups (userId, or
        // entityType+entityId together) - there's no entityType-only filter - so the only way to
        // find this entry through the real API is the unfiltered "return everything, newest
        // first" branch, narrowed here to Order-entityType entries with a matching action within
        // a recency window (so a stale createOrder entry from an earlier scenario in this same
        // suite run can't false-positive the match).
        String url = AUDIT_LOG_SERVICE_BASE_URL + "/audit-log";
        Instant recentSince = Instant.now().minus(Duration.ofSeconds(60));

        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        boolean found = false;
        String lastBody = null;
        while (Instant.now().isBefore(deadline) && !found) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + tokenClient.tokenFor("admin1", "password"))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                lastBody = response.body();
                JsonNode entries = objectMapper.readTree(lastBody);
                if (entries.isArray()) {
                    for (JsonNode entry : entries) {
                        boolean isOrderEntity = "Order".equals(entry.path("entityType").asText(null));
                        boolean actionMatches = entry.path("action").asText("").contains(actionFragment);
                        Instant timestamp = Instant.parse(entry.path("timestamp").asText());
                        if (isOrderEntity && actionMatches && timestamp.isAfter(recentSince)) {
                            found = true;
                            break;
                        }
                    }
                }
            } else {
                lastBody = "HTTP " + response.statusCode() + ": " + response.body();
            }
            if (!found) {
                Thread.sleep(1000);
            }
        }

        assertTrue(found,
                "No recent audit log entry for entityType=Order with action containing \"" + actionFragment
                        + "\" found within 30s (order id " + orderId + ", placed just before this step). "
                        + "Last response: " + lastBody);
    }
}
