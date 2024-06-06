package dev.mayankg.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@SuppressWarnings("unused")
public class MyMsgProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topicName;

    public void sendMessageToTopic(String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, message);
            future.whenComplete((result, throwable) -> {
                if (throwable == null) {
                    RecordMetadata recordMetadata = result.getRecordMetadata();
                    System.out.println("Sent Msg: {" + message + "] @ partition=["
                            + recordMetadata.partition() + "with offset="
                            + recordMetadata.offset() + "]" + "}");
                } else {
                    System.err.println("Unable to send the message=["
                            + message + "] due to : " + throwable.getMessage());
                }
            });
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
    }
}