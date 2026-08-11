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
        // title/culprit match reasoning, CORRECTED against a live-captured payload during final
        // review (see .superpowers/sdd/2026-08-10-exception-tracking/final-fix-report.md): the
        // original assumption here was that GlitchTip's `culprit` field would render as
        // "com.sanjay.ftgo.order.api.OrderController in triggerDiagnosticException" (per
        // sentry/culprit.py's generate_culprit()). A live-captured issue showed `culprit` is
        // actually an empty string for this event shape; the reliable field is
        // `metadata.filename` ("OrderController.java", set by GlitchTip's Java-platform event
        // processor from the top in-app stack frame), so matching moved there instead.
        // titleMatchSeen distinguishes "no matching title ever showed up" (capture pipeline
        // likely broken) from "title matched but filename didn't" (the match field assumption is
        // wrong) in the failure message below.
        boolean titleMatchSeen = false;
        String lastNonMatchingFilename = null;
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
                        String filename = issue.path("metadata").path("filename").asText("");
                        if (title.contains("IllegalStateException")) {
                            titleMatchSeen = true;
                            if (filename.contains("OrderController")) {
                                return;
                            }
                            lastNonMatchingFilename = filename;
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
                            + "but its metadata.filename never contained \"OrderController\" — last "
                            + "observed filename was: \"" + lastNonMatchingFilename + "\". The exception "
                            + "WAS captured; only the match-field assumption in this step appears wrong "
                            + "and should be updated to match.");
        }
        throw new AssertionError(
                "No GlitchTip issue titled IllegalStateException appeared within 30s — the "
                        + "capture pipeline (Sentry SDK -> GlitchTip ingest) likely did not fire, "
                        + "rather than this step's title/culprit matching being wrong.");
    }
}
