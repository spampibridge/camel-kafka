package it.bridgeconsulting.camel.kafka.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class IntegrationRoute extends EndpointRouteBuilder {
	
	public static final String SUCCESS_HANDLER_ROUTE = "successHandlerRoute";
	public static final String ERROR_HANDLER_ROUTE = "errorHandlerRoute";
	public static final String KAFKA_CONSUMER_ROUTE = "kafkaConsumerRoute";
	private static final String HEADER_LOG = "--> with headers: ${headers}";

	@Override
	public void configure() throws Exception {
		
		from(kafka("topic1")
				.brokers("localhost:9092")
				.groupId("myGroup"))
			.routeId(KAFKA_CONSUMER_ROUTE)
			.errorHandler(deadLetterChannel("seda:error")
				.maximumRedeliveries(5)
				.redeliveryDelay(1000)
				.retryAttemptedLogLevel(LoggingLevel.INFO)
				.loggingLevel(LoggingLevel.INFO)
				.logExhaustedMessageBody(true))
		    .log("Message received : ${body}")
		    .log("--> on the topic ${headers[kafka.TOPIC]}")
		    .log("--> on the partition ${headers[kafka.PARTITION]}")
		    .log("--> with the offset ${headers[kafka.OFFSET]}")
		    .log("--> with the key ${headers[kafka.KEY]}")
		    .log(HEADER_LOG)
		    .process(exchange -> {
		    	log.info("Processing message: {}", exchange.getIn().getBody(String.class));
		    	String body = exchange.getIn().getBody(String.class);
		    	if ("poison".equals(body)) {
		    		throw new IllegalArgumentException("Simulated processing error for body: " + body);
		    	}
		    })
		    .to(seda("success"));
		
		from(seda("error"))
		    .routeId(ERROR_HANDLER_ROUTE)
			.log("Error processing message: ${body}")
			.log(HEADER_LOG);
		
		from(seda("success"))
		    .routeId(SUCCESS_HANDLER_ROUTE)
			.log("Message processed: ${body}")
			.log(HEADER_LOG);
	}
}
