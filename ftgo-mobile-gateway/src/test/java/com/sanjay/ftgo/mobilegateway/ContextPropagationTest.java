package com.sanjay.ftgo.mobilegateway;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Hooks;

import static org.assertj.core.api.Assertions.assertThat;

// DirtiesContext(BEFORE_CLASS) forces Spring to discard any cached ApplicationContext left over
// from another test in this JVM/Gradle worker and build a fresh one for this class. Combined with
// disabling the (process-global, static) Reactor Hooks flag in @BeforeAll -- which runs before
// that fresh context is created -- this guarantees the assertion below can only pass because
// GatewayCommonAutoConfiguration's fallback bean re-enabled the flag during THIS test's context
// startup, not because a previous test in the same JVM happened to enable it first.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
class ContextPropagationTest {

    @BeforeAll
    static void resetGlobalReactorHooksState() {
        Hooks.disableAutomaticContextPropagation();
    }

    @Test
    void automaticContextPropagationIsEnabled() {
        assertThat(Hooks.isAutomaticContextPropagationEnabled()).isTrue();
    }
}
