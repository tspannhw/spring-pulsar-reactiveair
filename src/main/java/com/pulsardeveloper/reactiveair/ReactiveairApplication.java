package com.pulsardeveloper.reactiveair;

import com.github.javafaker.Faker;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.shade.com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Flux;

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

    @Value("${topic.name:reactivefaker}")
    String topicName;

    @Autowired
    ReactivePulsarTemplate<String> reactivePulsarTemplate;

    @Scheduled(initialDelay = 1000, fixedRate = 1000)
    public void getRows() {
        reactivePulsarTemplate.setSchema(Schema.STRING);

        for ( int rowCounter = 0; rowCounter < 20; rowCounter++ ) {
            System.out.println("sending " + rowCounter);
            reactivePulsarTemplate.newMessage(messageBuilder())
                    .withMessageCustomizer((mb) -> mb.key(UUID.randomUUID().toString()))
                    .withSenderCustomizer((sc) -> sc.accessMode(ProducerAccessMode.Shared))
                    .withSenderCustomizer((sc2) -> sc2.producerName("ReactiveProducer"))
                    .withSenderCustomizer((sc3) -> sc3.sendTimeout(Duration.ofSeconds(60L)))
                    .withSenderCustomizer((sc4) -> sc4.maxInflight(100))
                    .send().subscribe();
        }
    }

    @Bean
    ApplicationRunner runner(ReactivePulsarTemplate<String> pulsarTemplate) {

        pulsarTemplate.setSchema(Schema.STRING);

        System.out.println("runner");
        return (args) -> pulsarTemplate.newMessage(messageBuilder())
                .withMessageCustomizer((mb) -> mb.key(UUID.randomUUID().toString()))
                .withSenderCustomizer( (sc) -> sc.accessMode(ProducerAccessMode.Shared))
                .withSenderCustomizer( (sc2) -> sc2.producerName("ReactiveProducer"))
                .withSenderCustomizer( (sc3) -> sc3.sendTimeout(Duration.ofSeconds(60L)))
                .withSenderCustomizer( (sc4) -> sc4.maxInflight(100))
                .send().subscribe();
    }


    @ReactivePulsarListener(topics = "persistent://public/default/reactivefaker", stream = true)
    Flux<MessageResult<Void>> listen(Flux<Message<String>> messages) {
        return messages
                .doOnNext((msg) -> System.out.println("Stream Received: " + msg.getValue()))
                .map(MessageResult::acknowledge);
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

    @Override
    public void run(String... args) throws Exception {

    }
}