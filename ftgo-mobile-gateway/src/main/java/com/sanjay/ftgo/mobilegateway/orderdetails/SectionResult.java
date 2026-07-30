package com.sanjay.ftgo.mobilegateway.orderdetails;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "status")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SectionResult.Found.class, name = "FOUND"),
        @JsonSubTypes.Type(value = SectionResult.NotFound.class, name = "NOT_FOUND"),
        @JsonSubTypes.Type(value = SectionResult.Unavailable.class, name = "UNAVAILABLE")
})
public sealed interface SectionResult<T> {

    record Found<T>(T data) implements SectionResult<T> {}
    record NotFound<T>() implements SectionResult<T> {}
    record Unavailable<T>() implements SectionResult<T> {}
}
