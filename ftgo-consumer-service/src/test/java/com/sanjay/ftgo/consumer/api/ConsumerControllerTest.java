package com.sanjay.ftgo.consumer.api;

import com.sanjay.ftgo.consumer.domain.Consumer;
import com.sanjay.ftgo.consumer.domain.ConsumerRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import java.util.List;

// addFilters = false: this slice test predates Ch.11 security and exercises ConsumerController's
// business logic, not auth - it never sends a bearer token, so the Spring Security filter chain
// would 401 every request. Auth enforcement is verified at the e2e layer per the design spec.
@WebMvcTest(ConsumerController.class)
@AutoConfigureMockMvc(addFilters = false)
class ConsumerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ConsumerRepository consumerRepository;

    @MockitoBean
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        lenient().when(meterRegistry.counter(anyString())).thenReturn(org.mockito.Mockito.mock(Counter.class));
    }

    @Test
    void createsActiveConsumer() throws Exception {
        Consumer saved = new Consumer(7L, "E2E Consumer", true);
        when(consumerRepository.save(any(Consumer.class))).thenReturn(saved);

        mockMvc.perform(post("/consumers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name":"E2E Consumer","active":true}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(7))
                .andExpect(jsonPath("$.name").value("E2E Consumer"))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void createsInactiveConsumer() throws Exception {
        Consumer saved = new Consumer(8L, "Inactive E2E", false);
        when(consumerRepository.save(any(Consumer.class))).thenReturn(saved);

        mockMvc.perform(post("/consumers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name":"Inactive E2E","active":false}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.active").value(false));
    }

    @Test
    void createConsumerIncrementsConsumersCreatedCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ConsumerController withMetrics = new ConsumerController(consumerRepository, meterRegistry);
        when(consumerRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        withMetrics.createConsumer(new CreateConsumerRequest("Test", true));

        assertThat(meterRegistry.counter("consumers_created").count()).isEqualTo(1.0);
    }
}
