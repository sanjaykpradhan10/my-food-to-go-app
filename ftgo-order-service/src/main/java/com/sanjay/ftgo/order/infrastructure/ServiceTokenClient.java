package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.charset.StandardCharsets;

// Fetches and caches a client_credentials access token for order-service's own service
// identity (client "ftgo-order-service"), used to authenticate this service's outbound calls
// to other services' internal read endpoints. Not related to the per-user tokens gateway
// clients present on inbound requests - this is service-to-service, not user-to-service.
//
// The token URI/client credentials are externalized as properties (rather than hardcoded, as
// the task brief's sketch had them) so proxy integration tests can point this client at their
// own WireMock instance instead of a real authorization-server; production defaults match the
// Docker Compose service name ("authorization-server", not "ftgo-authorization-server" - see
// compose.yml, every other service's JWK_SET_URI already resolves against that bare name).
@Component
public class ServiceTokenClient {

    // Refresh this far before actual expiry so a request never races a token that expires
    // mid-flight; the authorization server issues 5-minute tokens (see AuthorizationServerConfig),
    // so a 30s margin leaves ample time for a fetch-and-retry without needing token expiry math
    // to be exact.
    private static final long REFRESH_MARGIN_SECONDS = 30;

    private final String tokenUri;
    private final String basicAuthHeader;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile String cachedToken;
    private volatile Instant cachedTokenExpiry = Instant.MIN;

    public ServiceTokenClient(
            @Value("${ftgo.service-token.token-uri:http://authorization-server:9000/oauth2/token}") String tokenUri,
            @Value("${ftgo.service-token.client-id:ftgo-order-service}") String clientId,
            @Value("${ftgo.service-token.client-secret:order-service-secret}") String clientSecret) {
        this.tokenUri = tokenUri;
        this.basicAuthHeader = Base64.getEncoder().encodeToString(
                (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
    }

    public String currentToken() {
        if (cachedToken != null && Instant.now().isBefore(cachedTokenExpiry.minusSeconds(REFRESH_MARGIN_SECONDS))) {
            return cachedToken;
        }
        lock.lock();
        try {
            if (cachedToken != null && Instant.now().isBefore(cachedTokenExpiry.minusSeconds(REFRESH_MARGIN_SECONDS))) {
                return cachedToken;
            }
            fetchToken();
            return cachedToken;
        } finally {
            lock.unlock();
        }
    }

    private void fetchToken() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenUri))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Authorization", "Basic " + basicAuthHeader)
                    .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Service token request failed: " + response.statusCode() + " " + response.body());
            }
            JsonNode json = objectMapper.readTree(response.body());
            cachedToken = json.get("access_token").asText();
            long expiresIn = json.get("expires_in").asLong();
            cachedTokenExpiry = Instant.now().plusSeconds(expiresIn);
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to fetch service token", e);
        }
    }
}
