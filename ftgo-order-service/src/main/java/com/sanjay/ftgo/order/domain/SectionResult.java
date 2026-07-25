package com.sanjay.ftgo.order.domain;

public sealed interface SectionResult<T> permits Found, NotFound, Unavailable {
}
