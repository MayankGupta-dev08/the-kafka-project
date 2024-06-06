package dev.mayankg.service;

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

    private static Logger logger = LoggerFactory.getLogger(MyMsgConsumer.class);

    /**
     * Consumes message
     *
     * @KafkaListener(topics = "tkp-demo")
     */
    @KafkaListener(topics = "#{@environment.getProperty('app.topic.name')}",
            groupId = "#{@environment.getProperty('consumer.group.id')}")
    public void consumeMessage(String message) {
        logger.info("consumer consumes the message => {}", message);
    }
}