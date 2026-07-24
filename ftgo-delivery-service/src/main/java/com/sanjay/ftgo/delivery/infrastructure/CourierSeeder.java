package com.sanjay.ftgo.delivery.infrastructure;

import com.sanjay.ftgo.delivery.domain.Courier;
import com.sanjay.ftgo.delivery.domain.CourierRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CourierSeeder implements CommandLineRunner {

    private final CourierRepository courierRepository;

    public CourierSeeder(CourierRepository courierRepository) {
        this.courierRepository = courierRepository;
    }

    @Override
    public void run(String... args) {
        if (courierRepository.count() > 0) {
            return;
        }
        courierRepository.save(new Courier("Alex"));
        courierRepository.save(new Courier("Bailey"));
        courierRepository.save(new Courier("Casey"));
    }
}
