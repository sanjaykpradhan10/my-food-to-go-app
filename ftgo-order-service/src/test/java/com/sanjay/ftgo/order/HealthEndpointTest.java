package com.sanjay.ftgo.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class HealthEndpointTest {

    @Autowired
    private MockMvc mockMvc;

    // order-service's src/test/resources/application.yml swaps in an H2 datasource (real
    // connection, so the "db" indicator reports UP) but leaves Kafka pointed at
    // localhost:9092, which nothing is listening on in a plain unit test — asserting the
    // overall aggregate `status` would be flaky (it goes DOWN because of the unreachable
    // Kafka indicator). Assert on the "db" component specifically instead; the Docker-based
    // end-to-end scenario (Task 3) is what proves the real, live "UP" aggregate.
    @Test
    void healthEndpointReportsDatabaseComponentUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").exists())
                .andExpect(jsonPath("$.components.db.status").value("UP"));
    }
}
