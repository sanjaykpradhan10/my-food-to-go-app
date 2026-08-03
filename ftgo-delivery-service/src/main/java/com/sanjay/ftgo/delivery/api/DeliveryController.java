package com.sanjay.ftgo.delivery.api;

import com.sanjay.ftgo.delivery.domain.Delivery;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEvent;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEventPublisher;
import com.sanjay.ftgo.delivery.domain.DeliveryNotFoundException;
import com.sanjay.ftgo.delivery.domain.DeliveryRepository;
import com.sanjay.ftgo.delivery.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/deliveries")
@Transactional
public class DeliveryController {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryDomainEventPublisher domainEventPublisher;

    public DeliveryController(DeliveryRepository deliveryRepository, DeliveryDomainEventPublisher domainEventPublisher) {
        this.deliveryRepository = deliveryRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @PreAuthorize("hasAnyRole('COURIER', 'ADMIN')")
    @PostMapping("/{deliveryId}/picked-up")
    public ResponseEntity<Void> pickedUp(@PathVariable Long deliveryId) {
        Delivery delivery = findDelivery(deliveryId);
        apply(delivery, delivery.pickUp());
        return ResponseEntity.ok().build();
    }

    @PreAuthorize("hasAnyRole('COURIER', 'ADMIN')")
    @PostMapping("/{deliveryId}/delivered")
    public ResponseEntity<Void> delivered(@PathVariable Long deliveryId) {
        Delivery delivery = findDelivery(deliveryId);
        apply(delivery, delivery.deliver());
        return ResponseEntity.ok().build();
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<DeliveryInfo> viewByOrderId(@PathVariable Long orderId) {
        return deliveryRepository.findByOrderId(orderId)
                .map(delivery -> new DeliveryInfo(
                        delivery.getId(), delivery.getOrderId(), delivery.getStatus().name(), delivery.getCourierId()))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private Delivery findDelivery(Long deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new DeliveryNotFoundException(deliveryId));
    }

    private void apply(Delivery delivery, List<DeliveryDomainEvent> events) {
        deliveryRepository.save(delivery);
        domainEventPublisher.publish(delivery, events);
    }

    @ExceptionHandler(DeliveryNotFoundException.class)
    public ResponseEntity<String> handleNotFound(DeliveryNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(UnsupportedStateTransitionException.class)
    public ResponseEntity<String> handleConflict(UnsupportedStateTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }
}
