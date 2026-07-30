package com.sanjay.ftgo.mobilegateway.orderdetails;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// Regression test for the bug found during live Docker e2e verification: NotFound and
// Unavailable are both zero-field records, so without a discriminator Jackson serializes
// both to "{}" and a client can't tell "sub-resource doesn't exist yet" from "backend down,
// circuit breaker fell back."
class SectionResultTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void foundSerializesWithStatusAndData() throws Exception {
        SectionResult<String> found = new SectionResult.Found<>("some-data");

        String json = mapper.writeValueAsString(found);

        assertThat(json).contains("\"status\":\"FOUND\"");
        assertThat(json).contains("\"data\":\"some-data\"");
    }

    @Test
    void notFoundSerializesWithDistinctStatus() throws Exception {
        SectionResult<String> notFound = new SectionResult.NotFound<>();

        String json = mapper.writeValueAsString(notFound);

        assertThat(json).contains("\"status\":\"NOT_FOUND\"");
    }

    @Test
    void unavailableSerializesWithDistinctStatus() throws Exception {
        SectionResult<String> unavailable = new SectionResult.Unavailable<>();

        String json = mapper.writeValueAsString(unavailable);

        assertThat(json).contains("\"status\":\"UNAVAILABLE\"");
    }

    @Test
    void notFoundAndUnavailableNoLongerSerializeIdentically() throws Exception {
        String notFoundJson = mapper.writeValueAsString(new SectionResult.NotFound<String>());
        String unavailableJson = mapper.writeValueAsString(new SectionResult.Unavailable<String>());

        assertThat(notFoundJson).isNotEqualTo(unavailableJson);
    }
}
