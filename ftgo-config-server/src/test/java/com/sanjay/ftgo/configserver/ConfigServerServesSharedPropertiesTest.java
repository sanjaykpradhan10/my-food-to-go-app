package com.sanjay.ftgo.configserver;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"test", "native"})
class ConfigServerServesSharedPropertiesTest {

    @LocalServerPort
    private int port;

    private final TestRestTemplate restTemplate = new TestRestTemplate();

    @Test
    void servesSharedPropertiesForAnyApplicationName() {
        String body = restTemplate.getForObject(
                "http://localhost:" + port + "/ftgo-order-service/default", String.class);

        assertThat(body).contains("\"outbox.poll-fixed-delay-ms\":\"2000\"");
        assertThat(body).contains("health, prometheus, refresh");
    }
}
