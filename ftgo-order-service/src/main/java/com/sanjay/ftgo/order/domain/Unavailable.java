package com.sanjay.ftgo.order.domain;

public record Unavailable<T>(String reason) implements SectionResult<T> {
}
