package com.sanjay.ftgo.mobilegateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Hooks;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ContextPropagationTest {

    @Test
    void automaticContextPropagationIsEnabled() {
        assertThat(Hooks.isAutomaticContextPropagationEnabled()).isTrue();
    }
}
