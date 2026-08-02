package com.sanjay.ftgo.publicgateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@AutoConfigureWebTestClient
class HealthEndpointTest {

    @Autowired
    private WebTestClient webTestClient;

    // The "test" profile (src/test/resources/application-test.yml) disables the Eureka client
    // entirely, so there's no discoveryComposite indicator to assert on here — this only proves
    // the endpoint is wired up and responding on the reactive stack. The Docker-based end-to-end
    // scenario (Task 3) is what proves the real "UP" aggregate with Eureka actually registered.
    @Test
    void healthEndpointRespondsOk() {
        webTestClient.get().uri("/actuator/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").exists();
    }
}
