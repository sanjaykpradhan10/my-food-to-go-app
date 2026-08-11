package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionTrackingStepDefinitions {

    private static final String ORDER_SERVICE_BASE_URL = "http://localhost:8082";
    // GlitchTip's REST API is Sentry-API-compatible; "ftgo" is both the organization slug and the
    // project slug the Task 1 provisioning script creates (glitchtip/provision.sh).
    private static final String GLITCHTIP_ISSUES_URL = "http://localhost:8000/api/0/organizations/ftgo/issues/";
    // glitchtip/provision.sh (Task 1/3) mints a GlitchTip internal API token scoped to
    // org:read/project:read/event:read/member:read and writes it to glitchtip/dsn.env, which is
    // bind-mounted host-side (compose's glitchtip-provisioner service mounts ./glitchtip, so the
    // file it writes lands directly on the host filesystem — no manual copy step, no host port
    // needed). GlitchTip's issues API returns 401/403 for unauthenticated or under-scoped
    // requests rather than an empty result, so this must never silently fall back to "no auth".
    //
    // No hardcoded token here on purpose — a prior version of this file committed a live token
    // to git history, which was revoked once discovered. GLITCHTIP_API_TOKEN env var wins if set
    // (e.g. CI exporting it explicitly); otherwise this reads it straight out of the host-side
    // dsn.env compose already produces, so a fresh `docker compose up` needs no manual step.
    private static final Path DSN_ENV_FILE =
            Path.of(System.getProperty("user.dir"), "..", "glitchtip", "dsn.env");
    private static final String GLITCHTIP_API_TOKEN = resolveGlitchTipApiToken();

    private static String resolveGlitchTipApiToken() {
        String fromEnv = System.getenv("GLITCHTIP_API_TOKEN");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        if (Files.isReadable(DSN_ENV_FILE)) {
            try {
                for (String line : Files.readAllLines(DSN_ENV_FILE)) {
                    if (line.startsWith("GLITCHTIP_API_TOKEN=")) {
                        String value = line.substring("GLITCHTIP_API_TOKEN=".length()).trim();
                        if (!value.isBlank()) {
                            return value;
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed reading " + DSN_ENV_FILE.toAbsolutePath(), e);
            }
        }
        throw new IllegalStateException(
                "GLITCHTIP_API_TOKEN is not set and could not be read from "
                        + DSN_ENV_FILE.toAbsolutePath()
                        + ". Run `docker compose up -d glitchtip-provisioner` (or the full stack) so "
                        + "glitchtip/provision.sh mints the token, or export GLITCHTIP_API_TOKEN "
                        + "explicitly before running this test.");
    }

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
        // title/culprit match reasoning (no live-captured payload was observed to confirm this —
        // see task-3-report.md): GlitchTip's culprit is computed by sentry/culprit.py's
        // generate_culprit(), which for a non-native platform formats the last in-app stack frame
        // as "%s in %s" % (frame.module, frame.function). The Sentry Java SDK sets `module` to
        // the fully-qualified declaring class, so the top frame here is expected to render as
        // "com.sanjay.ftgo.order.api.OrderController in triggerDiagnosticException" — hence
        // matching on the substring "OrderController" rather than requiring an exact string.
        // titleMatchSeen distinguishes "no matching title ever showed up" (capture pipeline
        // likely broken) from "title matched but culprit didn't" (this substring assumption is
        // wrong) in the failure message below.
        boolean titleMatchSeen = false;
        String lastNonMatchingCulprit = null;
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
                        if (title.contains("IllegalStateException")) {
                            titleMatchSeen = true;
                            if (culprit.contains("OrderController")) {
                                return;
                            }
                            lastNonMatchingCulprit = culprit;
                        }
                    }
                }
            } else if (response.statusCode() == 401 || response.statusCode() == 403) {
                throw new AssertionError(
                        "GlitchTip issues API rejected the request (HTTP " + response.statusCode()
                                + ") — check GLITCHTIP_API_TOKEN validity/scopes rather than assuming "
                                + "capture never happened: " + response.body());
            }
            Thread.sleep(1000);
        }
        if (titleMatchSeen) {
            throw new AssertionError(
                    "GlitchTip captured an IllegalStateException issue (title matched) within 30s, "
                            + "but its culprit never contained \"OrderController\" — last observed "
                            + "culprit was: \"" + lastNonMatchingCulprit + "\". The exception WAS "
                            + "captured; only the culprit-format assumption in this step appears wrong "
                            + "and should be updated to match.");
        }
        throw new AssertionError(
                "No GlitchTip issue titled IllegalStateException appeared within 30s — the "
                        + "capture pipeline (Sentry SDK -> GlitchTip ingest) likely did not fire, "
                        + "rather than this step's title/culprit matching being wrong.");
    }
}
