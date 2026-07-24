package com.sanjay.ftgo.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

// EnableScheduling is required for ftgo-common's OutboxPublisher.publishPendingEvents()
// (@Scheduled) to actually run — without it Spring silently never invokes the poller and
// outbox rows are written but never relayed to Kafka (sent_at stays NULL forever).
@SpringBootApplication
@EnableScheduling
public class FtgoDeliveryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoDeliveryServiceApplication.class, args);
    }
}
