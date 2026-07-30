package com.sanjay.ftgo.mobilegateway.orderdetails;

public record OrderDetails(
        SectionResult<String> order,
        SectionResult<String> ticket,
        SectionResult<String> authorization,
        SectionResult<String> delivery) {
}
