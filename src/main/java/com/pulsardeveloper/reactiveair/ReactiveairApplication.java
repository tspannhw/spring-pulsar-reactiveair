package com.pulsardeveloper.reactiveair;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MessageSpecBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
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

		String message = new StringBuilder().append("Hello world! ").append(uuidKey.toString()).toString();
		MessageSpecBuilder<String> messageSpecBuilder = MessageSpec.builder(message).key(uuidKey.toString());
		Mono<MessageId> messageId = messageSender
				.sendMessage(Mono.just(messageSpecBuilder.build()));

		System.out.println(messageSpecBuilder.build().toString());

		System.out.println("Complete.");
	}
}
