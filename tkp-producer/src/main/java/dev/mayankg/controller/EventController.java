package dev.mayankg.controller;

import dev.mayankg.dto.Customer;
import dev.mayankg.service.MyMsgProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SuppressWarnings("unused")
@RequestMapping("/producer-app")
public class EventController {

    private final MyMsgProducer producer;

    @Autowired
    public EventController(MyMsgProducer producer) {
        this.producer = producer;
    }

    /**
     * Publishes message
     *
     * @param message
     */
    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            producer.sendMessageToTopic(message);
            return ResponseEntity.ok("message published successfully...");
        } catch (Exception exception) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }

    /**
     * Publishes customer entity
     *
     * @param customer
     */
    @PostMapping("/publish")
    public ResponseEntity<?> publishEvent(@RequestBody Customer customer) {
        try {
            producer.sendMessageToTopic(customer);
            return ResponseEntity.ok("event published successfully...");
        } catch (Exception exception) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}