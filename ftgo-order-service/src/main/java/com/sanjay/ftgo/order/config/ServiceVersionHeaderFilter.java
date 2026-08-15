package com.sanjay.ftgo.order.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

import java.io.IOException;

// Exists solely as this sub-project's (Ch.12 B2) zero-downtime rollout evidence: k6 traces this
// header across a live rolling update to prove requests are never dropped and the version
// transition (e.g. 1.0.0 -> 1.1.0) is clean. Not a general-purpose versioning mechanism.
@Component
public class ServiceVersionHeaderFilter extends GenericFilterBean {

    private final String serviceVersion;

    public ServiceVersionHeaderFilter(@Value("${ftgo.service-version:unknown}") String serviceVersion) {
        this.serviceVersion = serviceVersion != null ? serviceVersion : "unknown";
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        ((HttpServletResponse) response).setHeader("X-Service-Version", serviceVersion);
        chain.doFilter(request, response);
    }
}
