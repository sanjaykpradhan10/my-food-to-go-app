// ftgo-delivery-service/src/test/java/com/sanjay/ftgo/delivery/api/DeliveryControllerTest.java
package com.sanjay.ftgo.delivery.api;

import com.sanjay.ftgo.delivery.domain.Delivery;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEventPublisher;
import com.sanjay.ftgo.delivery.domain.DeliveryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DeliveryController.class)
class DeliveryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DeliveryRepository deliveryRepository;

    @MockitoBean
    private DeliveryDomainEventPublisher domainEventPublisher;

    @Test
    void movesScheduledDeliveryToPickedUp() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/picked-up")).andExpect(status().isOk());
    }

    @Test
    void returns404WhenDeliveryNotFoundOnPickedUp() throws Exception {
        when(deliveryRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/deliveries/99/picked-up")).andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenPickingUpAlreadyPickedUpDelivery() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/picked-up")).andExpect(status().isConflict());
    }

    @Test
    void movesPickedUpDeliveryToDelivered() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/delivered")).andExpect(status().isOk());
    }

    @Test
    void returns409WhenDeliveringScheduledDelivery() throws Exception {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        mockMvc.perform(post("/deliveries/1/delivered")).andExpect(status().isConflict());
    }
}
