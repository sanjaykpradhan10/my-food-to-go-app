package com.sanjay.ftgo.mobilegateway.orderdetails;

public sealed interface SectionResult<T> {

    record Found<T>(T data) implements SectionResult<T> {}
    record NotFound<T>() implements SectionResult<T> {}
    record Unavailable<T>() implements SectionResult<T> {}
}
