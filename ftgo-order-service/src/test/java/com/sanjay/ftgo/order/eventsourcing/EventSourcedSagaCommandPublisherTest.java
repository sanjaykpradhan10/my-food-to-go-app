package com.sanjay.ftgo.order.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.KitchenCommand;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = "persistence.mode=event-sourcing")
@Import({EventSourcedSagaCommandPublisher.class, JacksonAutoConfiguration.class})
class EventSourcedSagaCommandPublisherTest {

    @Autowired
    private EventSourcedSagaCommandPublisher publisher;

    @Autowired
    private OrderSagaCommandRequestRepository repository;

    @Test
    void publishWritesAnUnpublishedRequestRow() {
        publisher.publish("kitchen.commands", "evt-1", "CreateTicket", 42L,
                new KitchenCommand("evt-1", "CreateTicket", 42L, 3, "CreateOrder"));

        var pending = repository.findByPublishedAtIsNullOrderByIdAsc();
        assertThat(pending).hasSize(1);
        assertThat(pending.get(0).getEventId()).isEqualTo("evt-1");
        assertThat(pending.get(0).getOrderId()).isEqualTo(42L);
        assertThat(pending.get(0).getTargetTopic()).isEqualTo("kitchen.commands");
        assertThat(pending.get(0).isPublished()).isFalse();
    }
}
