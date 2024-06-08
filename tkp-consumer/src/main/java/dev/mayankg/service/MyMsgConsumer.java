package dev.mayankg.service;

import dev.mayankg.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


/**
 * Kafka Consumer
 */
@Service
@SuppressWarnings("unused")
public class MyMsgConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MyMsgConsumer.class);

    /**
     * Consumes message
     *
     * @param message
     * @KafkaListener(topics = "tkp-demo")
     */
    @KafkaListener(topics = "#{@environment.getProperty('app.topic.name')}",
            groupId = "#{@environment.getProperty('consumer.group.id')}")
    public void consumeMessage(String message) {
        logger.info("consumer consumes the message => {}", message);
    }

    /**
     * Consumes event
     *
     * @param customer
     * @KafkaListener(topics = "tkp-demo")
     */
    @KafkaListener(topics = "#{@environment.getProperty('app.topic.name')}",
            groupId = "#{@environment.getProperty('consumer.group.id')}")
    public void consumeEvent(Customer customer) {
        if (customer != null) {
            logger.info("consumer consumes the customer => {}", customer);
        }
    }
}