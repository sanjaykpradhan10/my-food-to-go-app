package com.sanjay.ftgo.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class SchedulingEnabledTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void schedulingIsEnabled() {
        assertThat(applicationContext.getBeansOfType(ScheduledAnnotationBeanPostProcessor.class)).isNotEmpty();
    }
}
