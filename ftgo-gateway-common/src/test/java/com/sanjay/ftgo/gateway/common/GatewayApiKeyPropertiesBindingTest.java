package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that Spring Boot's constructor-binding machinery actually wires
 * gateway.api-key.value from a property source into GatewayApiKeyProperties,
 * rather than relying solely on direct `new GatewayApiKeyProperties(...)` construction
 * used by the other tests in this module. Tasks 5 and 6 depend on this exact binding
 * working from their own application.yml, so a regression here must fail loudly.
 */
class GatewayApiKeyPropertiesBindingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(GatewayCommonAutoConfiguration.class));

    @Test
    void bindsApiKeyValueFromPropertySource() {
        contextRunner.withPropertyValues("gateway.api-key.value=test-key")
                .run(context -> {
                    assertThat(context).hasSingleBean(GatewayApiKeyProperties.class);
                    assertThat(context.getBean(GatewayApiKeyProperties.class).value())
                            .isEqualTo("test-key");
                });
    }
}
