package com.sanjay.ftgo.accounting.domain;

import java.util.List;

public record AuthorizationResult(Authorization authorization, List<AuthorizationDomainEvent> events) {
}
