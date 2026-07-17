package com.sanjay.ftgo.consumer.infrastructure;

import com.sanjay.ftgo.consumer.domain.Consumer;
import com.sanjay.ftgo.consumer.domain.ConsumerRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class DataSeeder implements CommandLineRunner {

    private final ConsumerRepository consumerRepository;

    public DataSeeder(ConsumerRepository consumerRepository) {
        this.consumerRepository = consumerRepository;
    }

    @Override
    public void run(String... args) {
        if (consumerRepository.count() > 0) {
            return;
        }
        consumerRepository.save(new Consumer("Sanjay", true));
        consumerRepository.save(new Consumer("Blocked Consumer", false));
    }
}
