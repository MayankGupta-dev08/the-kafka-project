package dev.mayankg.service;

import dev.mayankg.dto.Customer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Producer
 */
@Service
@SuppressWarnings("unused")
public class MyMsgProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topicName;

    /**
     * Produces message
     *
     * @param message
     */
    public void sendMessageToTopic(String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, message);
            future.whenComplete((result, throwable) -> {
                if (throwable == null) {
                    RecordMetadata recordMetadata = result.getRecordMetadata();
                    System.out.println("Sent Msg=[" + message + "] @ partition=["
                            + recordMetadata.partition() + "] with offset=[" + recordMetadata.offset() + "]");
                } else {
                    System.err.println("Unable to send the message=["
                            + message + "] due to : " + throwable.getMessage());
                }
            });
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
    }

    /**
     * Produces message
     *
     * @param customer
     */
    public void sendMessageToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, customer);
            future.whenComplete((result, throwable) -> {
                if (throwable == null) {
                    RecordMetadata recordMetadata = result.getRecordMetadata();
                    System.out.println("Sent Msg=[" + customer.toString() + "] @ partition=["
                            + recordMetadata.partition() + "] with offset=[" + recordMetadata.offset() + "]");
                } else {
                    System.err.println("Unable to send the message=["
                            + customer.toString() + "] due to : " + throwable.getMessage());
                }
            });
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
    }
}