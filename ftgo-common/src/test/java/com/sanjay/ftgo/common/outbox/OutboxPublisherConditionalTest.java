package com.sanjay.ftgo.common.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OutboxPublisherConditionalTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withBean(PropertySourcesPlaceholderConfigurer.class)
            .withUserConfiguration(CollaboratorConfig.class, OutboxPublisher.class);

    @Test
    void beanExistsWhenPublishModeIsPolling() {
        contextRunner.withPropertyValues("outbox.publish-mode=polling")
                .run(context -> assertThat(context).hasSingleBean(OutboxPublisher.class));
    }

    @Test
    void beanExistsWhenPublishModeIsUnset() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(OutboxPublisher.class));
    }

    @Test
    void beanAbsentWhenPublishModeIsCdc() {
        contextRunner.withPropertyValues("outbox.publish-mode=cdc")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxPublisher.class));
    }

    @Configuration
    static class CollaboratorConfig {

        @Bean
        OutboxEventRepository outboxEventRepository() {
            return mock(OutboxEventRepository.class);
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }
    }
}
