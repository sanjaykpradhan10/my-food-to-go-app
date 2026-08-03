package com.sanjay.ftgo.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TokenClient {

    private static final String TOKEN_URL = "http://localhost:9000/oauth2/token";
    private static final String CLIENT_CREDENTIALS =
            Base64.getEncoder().encodeToString("ftgo-gateway:gateway-secret".getBytes());

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, String> tokenCache = new ConcurrentHashMap<>();

    public String tokenFor(String username, String password) throws Exception {
        return tokenCache.computeIfAbsent(username, u -> {
            try {
                return fetchToken(u, password);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private String fetchToken(String username, String password) throws Exception {
        String body = "grant_type=password&username=" + username + "&password=" + password;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(TOKEN_URL))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Authorization", "Basic " + CLIENT_CREDENTIALS)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Token request failed: " + response.statusCode() + " " + response.body());
        }
        JsonNode json = objectMapper.readTree(response.body());
        return json.get("access_token").asText();
    }
}
