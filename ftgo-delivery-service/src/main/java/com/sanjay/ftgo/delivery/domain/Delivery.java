package com.sanjay.ftgo.delivery.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.List;

@Entity
@Table(name = "deliveries")
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    private Long restaurantId;

    private Long courierId;

    @Enumerated(EnumType.STRING)
    private DeliveryStatus status;

    protected Delivery() {
    }

    private Delivery(Long orderId, Long restaurantId, Long courierId, DeliveryStatus status) {
        this.orderId = orderId;
        this.restaurantId = restaurantId;
        this.courierId = courierId;
        this.status = status;
    }

    // No Delivery is ever constructed outside SCHEDULED - a decline never persists a row
    // (mirrors Ticket.createTicket/TicketCreationFailed), so there's no separate "pending"
    // starting state to guard against here.
    public static DeliveryScheduleResult schedule(Long orderId, Long restaurantId, Long courierId) {
        Delivery delivery = new Delivery(orderId, restaurantId, courierId, DeliveryStatus.SCHEDULED);
        return new DeliveryScheduleResult(delivery, List.of(new DeliveryScheduledEvent(orderId, courierId)));
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public Long getCourierId() {
        return courierId;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public List<DeliveryDomainEvent> pickUp() {
        if (status != DeliveryStatus.SCHEDULED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = DeliveryStatus.PICKED_UP;
        return List.of(new DeliveryPickedUpEvent(orderId));
    }

    public List<DeliveryDomainEvent> deliver() {
        if (status != DeliveryStatus.PICKED_UP) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = DeliveryStatus.DELIVERED;
        return List.of(new DeliveryDeliveredEvent(orderId));
    }

    // Legal only from SCHEDULED - reused for both a real Cancel Order request and every
    // Create Order compensation path (consumer/kitchen/accounting failure), same as
    // Ticket.cancel() serving both roles.
    public List<DeliveryDomainEvent> cancel() {
        if (status != DeliveryStatus.SCHEDULED) {
            throw new UnsupportedStateTransitionException(status);
        }
        this.status = DeliveryStatus.CANCELLED;
        return List.of(new DeliveryCancelledEvent(orderId));
    }
}
