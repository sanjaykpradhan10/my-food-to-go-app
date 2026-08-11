package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionTrackingStepDefinitions {

    private static final String ORDER_SERVICE_BASE_URL = "http://localhost:8082";
    // GlitchTip's REST API is Sentry-API-compatible; "ftgo" is both the organization slug and the
    // project slug the Task 1 provisioning script creates (glitchtip/provision.sh).
    private static final String GLITCHTIP_ISSUES_URL = "http://localhost:8000/api/0/organizations/ftgo/issues/";
    // A GlitchTip internal API token scoped to org:read/project:read/event:read/member:read,
    // minted once for this e2e user (see Task 3 verification notes) — GlitchTip's issues API
    // returns 403 Permission denied for unauthenticated or under-scoped requests, unlike the
    // "empty result" you might expect from a REST API with no matching records.
    private static final String GLITCHTIP_API_TOKEN =
            System.getenv().getOrDefault("GLITCHTIP_API_TOKEN",
                    "d277c3fc6042fcd41a965b92e4158f7d4305c4d17c99bea0dbb561648c5f3b66");

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TokenClient tokenClient = new TokenClient();

    private int diagnosticResponseStatus;

    @When("an admin triggers the order-service diagnostic exception endpoint")
    public void anAdminTriggersTheDiagnosticExceptionEndpoint() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ORDER_SERVICE_BASE_URL + "/orders/_diagnostics/trigger-exception"))
                .header("Authorization", "Bearer " + tokenClient.tokenFor("admin1", "password"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        diagnosticResponseStatus = response.statusCode();
    }

    @Then("the diagnostic endpoint responds with a server error")
    public void theDiagnosticEndpointRespondsWithAServerError() {
        assertEquals(500, diagnosticResponseStatus);
    }

    @Then("GlitchTip eventually reports an IllegalStateException issue for ftgo-order-service")
    public void glitchtipEventuallyReportsAnIssue() throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(GLITCHTIP_ISSUES_URL + "?query=IllegalStateException"))
                    .header("Authorization", "Bearer " + GLITCHTIP_API_TOKEN)
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JsonNode issues = objectMapper.readTree(response.body());
                if (issues.isArray() && !issues.isEmpty()) {
                    for (JsonNode issue : issues) {
                        String title = issue.path("title").asText("");
                        String culprit = issue.path("culprit").asText("");
                        if (title.contains("IllegalStateException")
                                && culprit.contains("OrderController")) {
                            return;
                        }
                    }
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("Expected a GlitchTip issue for IllegalStateException in OrderController within 30s");
    }
}
