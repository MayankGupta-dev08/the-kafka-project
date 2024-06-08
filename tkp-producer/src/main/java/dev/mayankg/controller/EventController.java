package dev.mayankg.controller;

import dev.mayankg.dto.Customer;
import dev.mayankg.service.MyMsgProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@SuppressWarnings("unused")
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private MyMsgProducer publisher;

    /**
     * Publishes message
     *
     * @param message
     */
    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            for (int i = 1; i <= 10000; i++) {
                publisher.sendMessageToTopic(message + " : " + i);
            }
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
            publisher.sendMessageToTopic(customer);
            return ResponseEntity.ok("event published successfully...");
        } catch (Exception exception) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}