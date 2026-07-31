package com.sanjay.ftgo.order.contracttest;

import com.sanjay.ftgo.common.contracttest.KafkaContractTestSupport;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.OutboxPublisher;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderDomainEventPublisher;
import com.sanjay.ftgo.order.domain.OrderEventSerializer;
import com.sanjay.ftgo.order.domain.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.test.context.EmbeddedKafka;

// order-service's real @SpringBootApplication pulls in Eureka, resilience4j and the REST
// proxies for the other services - none of which this pub/sub contract needs and some of
// which fail fast without a running discovery server. TestConfig below is a narrower slice:
// explicit @Import of only the two beans this contract exercises (OrderDomainEventPublisher,
// OrderEventSerializer), not a package component scan - scanBasePackageClasses(domain package)
// also picks up OrderService, which needs RestaurantServicePort/other saga ports this test
// never wires, and fails bean creation. Explicit entity/repository scans cover the JPA classes
// the outbox actually touches (Order, OutboxEvent), the same trimming approach
// OrderRepositoryTest takes via @DataJpaTest.
// @EmbeddedKafka must be repeated directly here: it does not propagate through
// @Import(KafkaContractTestSupport.class) - Spring's TestContextAnnotationUtils only walks the
// test-class hierarchy, not @Import-reached classes (see KafkaContractTestSupport's own doc).
@SpringBootTest(classes = MessagingBase.TestConfig.class)
@EmbeddedKafka(partitions = 1, topics = {
        KafkaContractTestSupport.TOPIC_ORDER_EVENTS,
        KafkaContractTestSupport.TOPIC_KITCHEN_COMMANDS,
        KafkaContractTestSupport.TOPIC_SAGA_REPLIES})
@AutoConfigureMessageVerifier
@Import(KafkaContractTestSupport.class)
public abstract class MessagingBase {

    @Autowired
    private OrderDomainEventPublisher orderDomainEventPublisher;

    @Autowired
    private OutboxPublisher outboxPublisher;

    protected void orderCreated() {
        Order order = ContractFixtures.sampleOrder();
        orderDomainEventPublisher.publishOrderCreated(order, "11111111-1111-1111-1111-111111111111");
        outboxPublisher.publishPendingEvents();
    }

    @EnableAutoConfiguration
    @Import({OrderDomainEventPublisher.class, OrderEventSerializer.class})
    @EntityScan(basePackageClasses = {Order.class, OutboxEvent.class})
    @EnableJpaRepositories(basePackageClasses = {OrderRepository.class, OutboxEventRepository.class})
    static class TestConfig {
    }
}
