package com.sanjay.ftgo.orderhistory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

// This service's four @KafkaListeners (OrderEventListener/KitchenEventListener/
// AccountingEventListener/DeliveryEventListener) run on independent consumer threads and can
// race to update the same order_views row (e.g. OrderCreated and TicketCreated for the same
// orderId landing close together). OrderView's @Version field makes the losing writer's
// EntityManager.merge() throw OptimisticLockingFailureException instead of silently
// overwriting the winner's columns - this factory retries the whole listener invocation a
// few times with a short backoff so the loser gets a fresh findById() (each handler method
// re-reads at the top, since the retried invocation is a brand new @Transactional call) and
// sees the other thread's already-committed write rather than clobbering it. Overriding
// Spring Boot's default listener container factory bean this way (rather than autoconfiguring
// it away) is the documented approach for customizing retry behavior while keeping every other
// property (bootstrap-servers, group-id, deserializers) sourced from application.yml.
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // 3 retries (4 attempts total), 200ms apart - long enough for the other thread's
        // transaction (a handful of simple column updates) to have already committed by the retry.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(200L, 3)));
        return factory;
    }
}
