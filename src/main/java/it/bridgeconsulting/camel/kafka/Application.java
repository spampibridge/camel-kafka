package it.bridgeconsulting.camel.kafka;

import org.apache.camel.ProducerTemplate;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
    ApplicationRunner runner(ProducerTemplate template) {
        return _ -> {
        	log.info("Sending test message to Kafka topic...");
        	template.sendBodyAndHeader("kafka:topic1?brokers=localhost:9092", "testmessage", "traceparent", "00-mytrace-b7ad6b7169203331-01");
        };
    }
}
