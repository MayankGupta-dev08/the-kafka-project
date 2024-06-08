package dev.mayankg;

import dev.mayankg.dto.Customer;
import dev.mayankg.service.MyMsgConsumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;

@Slf4j
@Testcontainers
@AllArgsConstructor
@SuppressWarnings("unused")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TKPConsumerAppTest {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:latest";

    @Container
    private static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                    .withStartupTimeout(Duration.ofMinutes(2));

    @Autowired
    private MyMsgConsumer kafkaMsgConsumer;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @DynamicPropertySource
    public static void configureKafkaProperties(DynamicPropertyRegistry propertyRegistry) {
        propertyRegistry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        propertyRegistry.add(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class::getName);
        propertyRegistry.add(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class::getName);
    }

    @Test
    void testSendStringAsKafkaMessage() {
        log.info("TKPConsumerAppTest#testSendStringAsKafkaMessage :: sending 10 string messages...");
        IntStream.rangeClosed(1, 10).forEachOrdered(i -> {
            kafkaTemplate.send("tkp-topic-main", "testUser testing, instance : " + i);
        });
        await().pollInterval(Duration.ofSeconds(3))
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                });
        log.info("TKPConsumerAppTest#testSendStringAsKafkaMessage :: successfully sent 10 string messages...");
    }

    @Test
    void testSendCustomerObjectAsKafkaMessage() {
        log.info("TKPConsumerAppTest#testSendCustomerObjectAsKafkaMessage :: sending 10 messages...");
        IntStream.rangeClosed(0, 9).forEachOrdered(i -> {
            Customer tempCustomer = new Customer(i, "testUser" + i, "tu@test.com", "9100-000-00" + i);
            kafkaTemplate.send("tkp-topic-main", tempCustomer);
        });
        await().pollInterval(Duration.ofSeconds(3))
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                });
        log.info("TKPConsumerAppTest#testSendCustomerObjectAsKafkaMessage :: successfully sent 10 messages...");
    }

}