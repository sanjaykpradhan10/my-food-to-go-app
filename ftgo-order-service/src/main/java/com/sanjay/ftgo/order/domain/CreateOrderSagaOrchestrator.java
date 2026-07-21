package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class CreateOrderSagaOrchestrator {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository;
    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public CreateOrderSagaOrchestrator(CreateOrderSagaInstanceRepository sagaInstanceRepository,
                                        OrderRepository orderRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void start(Order order) {
        int totalQuantity = totalQuantity(order.getLineItems());
        sagaInstanceRepository.save(new CreateOrderSagaInstance(order.getId(), totalQuantity));

        String verifyEventId = UUID.randomUUID().toString();
        publishCommand("consumer.commands", verifyEventId, "VerifyConsumerCommand", order.getId(),
                new VerifyConsumerCommand(verifyEventId, order.getId(), order.getConsumerId()));

        String createTicketEventId = UUID.randomUUID().toString();
        publishCommand("kitchen.commands", createTicketEventId, "CreateTicket", order.getId(),
                new KitchenCommand(createTicketEventId, "CreateTicket", order.getId(), totalQuantity));
    }

    @Transactional
    public void handleReply(String eventId, String participant, String eventType, Long orderId, String reason) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        CreateOrderSagaInstance instance = sagaInstanceRepository.findById(orderId).orElse(null);
        if (instance == null) {
            return;
        }

        if (instance.isFailed()) {
            if ("kitchen".equals(participant) && "TicketCreated".equals(eventType)) {
                sendCancelTicket(orderId);
            }
            return;
        }

        switch (participant) {
            case "consumer" -> handleConsumerReply(instance, eventType);
            case "kitchen" -> handleKitchenReply(instance, eventType);
            case "accounting" -> handleAccountingReply(instance, eventType);
            default -> { }
        }
    }

    private void handleConsumerReply(CreateOrderSagaInstance instance, String eventType) {
        if ("ConsumerVerificationFailed".equals(eventType)) {
            fail(instance);
            return;
        }
        instance.markConsumerVerified();
        sagaInstanceRepository.save(instance);
        tryAuthorize(instance);
    }

    private void handleKitchenReply(CreateOrderSagaInstance instance, String eventType) {
        if ("TicketCreationFailed".equals(eventType)) {
            fail(instance);
            return;
        }
        instance.markTicketCreated();
        sagaInstanceRepository.save(instance);
        tryAuthorize(instance);
    }

    private void handleAccountingReply(CreateOrderSagaInstance instance, String eventType) {
        Order order = orderRepository.findById(instance.getOrderId()).orElse(null);
        if (order == null) {
            return;
        }
        if ("CardAuthorized".equals(eventType)) {
            if (order.getStatus() == OrderStatus.APPROVAL_PENDING) {
                order.noteApproved();
                orderRepository.save(order);
            }
            String eventId = UUID.randomUUID().toString();
            publishCommand("kitchen.commands", eventId, "ConfirmTicket", instance.getOrderId(),
                    new KitchenCommand(eventId, "ConfirmTicket", instance.getOrderId(), null));
        } else {
            rejectOrder(order);
            sendCancelTicket(instance.getOrderId());
        }
    }

    private void tryAuthorize(CreateOrderSagaInstance instance) {
        if (!instance.isConsumerVerified() || !instance.isTicketCreated()) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        publishCommand("accounting.commands", eventId, "AuthorizeCard", instance.getOrderId(),
                new AuthorizeCardCommand(eventId, instance.getOrderId(), instance.getTotalQuantity()));
    }

    private void fail(CreateOrderSagaInstance instance) {
        instance.markFailed();
        sagaInstanceRepository.save(instance);

        Order order = orderRepository.findById(instance.getOrderId()).orElse(null);
        if (order != null) {
            rejectOrder(order);
        }

        if (instance.isTicketCreated()) {
            sendCancelTicket(instance.getOrderId());
        }
    }

    private void sendCancelTicket(Long orderId) {
        String eventId = UUID.randomUUID().toString();
        publishCommand("kitchen.commands", eventId, "CancelTicket", orderId,
                new KitchenCommand(eventId, "CancelTicket", orderId, null));
    }

    private void rejectOrder(Order order) {
        if (order.getStatus() == OrderStatus.APPROVAL_PENDING) {
            order.noteRejected();
            orderRepository.save(order);
        }
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }

    private void publishCommand(String topic, String eventId, String eventType, Long orderId, Object command) {
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, topic, toJson(command)));
    }

    private String toJson(Object command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga command", e);
        }
    }
}
