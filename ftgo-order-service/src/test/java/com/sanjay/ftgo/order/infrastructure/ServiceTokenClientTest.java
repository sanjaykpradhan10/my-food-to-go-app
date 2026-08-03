package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

class ServiceTokenClientTest {

    private static final int PORT = 8199;

    private WireMockServer wireMockServer;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(PORT);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    private ServiceTokenClient newClient() {
        return new ServiceTokenClient(
                "http://localhost:" + PORT + "/oauth2/token", "ftgo-order-service", "order-service-secret");
    }

    @Test
    void returnsAccessTokenFromResponse() {
        wireMockServer.stubFor(post(urlEqualTo("/oauth2/token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"token-1\",\"expires_in\":300}")));

        ServiceTokenClient client = newClient();

        assertThat(client.currentToken()).isEqualTo("token-1");
    }

    @Test
    void cachesTokenAndDoesNotReissueRequestOnImmediateSecondCall() {
        wireMockServer.stubFor(post(urlEqualTo("/oauth2/token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"token-1\",\"expires_in\":300}")));

        ServiceTokenClient client = newClient();

        assertThat(client.currentToken()).isEqualTo("token-1");
        assertThat(client.currentToken()).isEqualTo("token-1");

        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/oauth2/token")));
    }

    @Test
    void refetchesTokenAfterExpiry() throws InterruptedException {
        // 1s expiry plus the client's 30s refresh margin means the cached token is already
        // considered stale as soon as it's stored, so the very next call re-fetches - this
        // exercises the refresh path without needing a sleep anywhere near a real 5-minute TTL.
        wireMockServer.stubFor(post(urlEqualTo("/oauth2/token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"token-1\",\"expires_in\":1}")));

        ServiceTokenClient client = newClient();
        assertThat(client.currentToken()).isEqualTo("token-1");

        wireMockServer.stubFor(post(urlEqualTo("/oauth2/token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"token-2\",\"expires_in\":300}")));

        Thread.sleep(50);
        assertThat(client.currentToken()).isEqualTo("token-2");

        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/oauth2/token")));
    }
}
