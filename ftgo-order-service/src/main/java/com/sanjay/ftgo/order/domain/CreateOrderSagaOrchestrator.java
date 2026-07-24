package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class CreateOrderSagaOrchestrator {

    private final CreateOrderSagaInstanceRepository sagaInstanceRepository;
    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public CreateOrderSagaOrchestrator(CreateOrderSagaInstanceRepository sagaInstanceRepository,
                                        OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        int totalQuantity = totalQuantity(order.getLineItems());
        sagaInstanceRepository.save(new CreateOrderSagaInstance(order.getId(), totalQuantity));

        String verifyEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("consumer.commands", verifyEventId, "VerifyConsumerCommand", order.getId(),
                new VerifyConsumerCommand(verifyEventId, order.getId(), order.getConsumerId()));

        String createTicketEventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", createTicketEventId, "CreateTicket", order.getId(),
                new KitchenCommand(createTicketEventId, "CreateTicket", order.getId(), totalQuantity, "CreateOrder"));
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
        Long orderId = instance.getOrderId();
        if ("CardAuthorized".equals(eventType)) {
            orderTransitions.approve(orderId, UUID.randomUUID().toString());
            String eventId = UUID.randomUUID().toString();
            sagaCommandPublisher.publish("kitchen.commands", eventId, "ConfirmTicket", orderId,
                    new KitchenCommand(eventId, "ConfirmTicket", orderId, null, "CreateOrder"));
        } else {
            orderTransitions.reject(orderId, UUID.randomUUID().toString());
            sendCancelTicket(orderId);
        }
    }

    private void tryAuthorize(CreateOrderSagaInstance instance) {
        if (!instance.isConsumerVerified() || !instance.isTicketCreated()) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("accounting.commands", eventId, "AuthorizeCard", instance.getOrderId(),
                new AccountingCommand(eventId, "AuthorizeCard", instance.getOrderId(), instance.getTotalQuantity(), "CreateOrder"));
    }

    private void fail(CreateOrderSagaInstance instance) {
        instance.markFailed();
        sagaInstanceRepository.save(instance);

        orderTransitions.reject(instance.getOrderId(), UUID.randomUUID().toString());

        if (instance.isTicketCreated()) {
            sendCancelTicket(instance.getOrderId());
        }
    }

    private void sendCancelTicket(Long orderId) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", eventId, "CancelTicket", orderId,
                new KitchenCommand(eventId, "CancelTicket", orderId, null, "CreateOrder"));
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }
}
