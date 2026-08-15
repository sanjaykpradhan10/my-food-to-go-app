package com.sanjay.ftgo.order.config;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ServiceVersionHeaderFilterTest {

    @Test
    void setsConfiguredVersionOnResponseHeader() throws Exception {
        ServiceVersionHeaderFilter filter = new ServiceVersionHeaderFilter("1.1.0");
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        assertThat(response.getHeader("X-Service-Version")).isEqualTo("1.1.0");
        verify(chain).doFilter(request, response);
    }

    @Test
    void defaultsToUnknownWhenPropertyMissing() throws Exception {
        ServiceVersionHeaderFilter filter = new ServiceVersionHeaderFilter(null);
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        assertThat(response.getHeader("X-Service-Version")).isEqualTo("unknown");
    }
}
