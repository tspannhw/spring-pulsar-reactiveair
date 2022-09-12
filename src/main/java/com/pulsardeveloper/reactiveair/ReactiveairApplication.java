package com.pulsardeveloper.reactiveair;

import com.github.javafaker.Faker;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MessageSpecBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.apache.pulsar.shade.com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@SpringBootApplication
public class ReactiveairApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(ReactiveairApplication.class);


    public static void main(String[] args) {
        SpringApplication.run(ReactiveairApplication.class, args);
    }

    @Autowired
    PulsarClient pulsarClient;

    @Value("${topic.name:airquality}")
    String topicName;

    @Override
    public void run(String... args) {

        ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);

        ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic(topicName)
                .maxInflight(100)
                .producerName("reactiveProducer")
                .sendTimeout(Duration.ofSeconds(60L))
                .accessMode(ProducerAccessMode.Shared)
                .build();

        UUID uuidKey = UUID.randomUUID();
        String message = messageBuilder();
        MessageSpecBuilder<String> messageSpecBuilder =
                MessageSpec.builder(message).key(uuidKey.toString());
        Mono<MessageId> messageId = messageSender
                .sendMessage(Mono.just(messageSpecBuilder.build()));
        System.out.println("Sent " + message);
    }

    public String messageBuilder() {
        Faker faker = new Faker();
        JsonObject jsonObject = new JsonObject();

        try {
            String stateName = faker.address().stateAbbr();
            jsonObject.addProperty("fullName", faker.name().fullName());
            jsonObject.addProperty("latitude",faker.address().latitude());
            jsonObject.addProperty("longitude", faker.address().longitude());
            jsonObject.addProperty("buildingNumber", faker.address().buildingNumber());
            jsonObject.addProperty("streetName", faker.address().streetAddressNumber() + " " + faker.address().streetName());
            jsonObject.addProperty("city", faker.address().city());
            jsonObject.addProperty("state", stateName);
            jsonObject.addProperty("country", faker.address().country());
            jsonObject.addProperty("zipCode", faker.address().zipCode());
            jsonObject.addProperty("product", faker.commerce().productName());
            jsonObject.addProperty("industry", faker.company().industry());
            jsonObject.addProperty("IBAN", faker.finance().iban());
            jsonObject.addProperty("creditCard", faker.finance().creditCard());
            jsonObject.addProperty("ipAddress", faker.internet().ipV4Address());
            jsonObject.addProperty("macAddress", faker.internet().macAddress());
            jsonObject.addProperty("cellPhone", faker.phoneNumber().cellPhone());
            jsonObject.addProperty("stock", faker.stock().nyseSymbol());
            jsonObject.addProperty("appName", faker.app().name());
            jsonObject.addProperty("price", faker.commerce().price());
            jsonObject.addProperty("stockPrice", faker.number().numberBetween(100, 150));
            jsonObject.addProperty("memory", Runtime.getRuntime().freeMemory());
        } catch (Exception e) {
            e.printStackTrace();
            return e.getLocalizedMessage();
        }

        return jsonObject.toString();
    }
}
