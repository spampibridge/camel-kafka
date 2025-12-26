package it.bridgeconsulting.camel.kafka;

import static org.apache.camel.builder.AdviceWith.adviceWith;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import it.bridgeconsulting.camel.kafka.route.IntegrationRoute;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
@CamelSpringBootTest
class IntegrationTest {

	private static final String TRACE_ID = "00-mytrace-b7ad6b7169203331-01";
	private static final String TRACEPARENT = "traceparent";
	private CamelContext camelContext;
    private ProducerTemplate producer;
    
    @MockitoBean
    private ApplicationRunner runner; // Mock the ApplicationRunner to prevent it from sending messages during tests
	
	@Autowired
	public IntegrationTest(CamelContext camelContext, ProducerTemplate producer) {
		super();
		this.camelContext = camelContext;
		this.producer = producer;
	}
	
	@Test
	void testKafkaIntegration() throws Exception {
		
		mockFlowDependencies();
		
		NotifyBuilder notifyCompletedSuccess = new NotifyBuilder(camelContext)
			.fromRoute(IntegrationRoute.SUCCESS_HANDLER_ROUTE)
    		.whenExactlyCompleted(1)
    		.create();
		
		NotifyBuilder notifyCompletedError = new NotifyBuilder(camelContext)
			.fromRoute(IntegrationRoute.ERROR_HANDLER_ROUTE)
    		.whenExactlyCompleted(1)
    		.create();
		
		MockEndpoint successMock = camelContext.getEndpoint("mock:successHandler", MockEndpoint.class);
		MockEndpoint errorMock = camelContext.getEndpoint("mock:errorHandler", MockEndpoint.class);
		
		successMock.expectedHeaderReceived(TRACEPARENT, TRACE_ID.getBytes(StandardCharsets.UTF_8));
		successMock.expectedMessageCount(1);
		errorMock.expectedHeaderReceived(TRACEPARENT, TRACE_ID.getBytes(StandardCharsets.UTF_8));
		errorMock.expectedMessageCount(1);
		
		producer.sendBodyAndHeader(
				"kafka:topic1?brokers=localhost:9092", 
				"testmessage", 
				TRACEPARENT, 
				TRACE_ID);
		producer.sendBodyAndHeader(
				"kafka:topic1?brokers=localhost:9092", 
				"poison", 
				TRACEPARENT, 
				TRACE_ID);
        
        boolean completedSuccess = notifyCompletedSuccess.matches(10, TimeUnit.SECONDS);
        assertThat(completedSuccess).isTrue();
        
        boolean completedError = notifyCompletedError.matches(10, TimeUnit.SECONDS);
        assertThat(completedError).isTrue();
        
        successMock.assertIsSatisfied();
        errorMock.assertIsSatisfied();
	}

	private void mockFlowDependencies() throws Exception {
		
		adviceWith(camelContext, IntegrationRoute.SUCCESS_HANDLER_ROUTE, a -> {
			a.weaveAddLast().to("mock:successHandler");
		});
		
		adviceWith(camelContext, IntegrationRoute.ERROR_HANDLER_ROUTE, a -> {
			a.weaveAddLast().to("mock:errorHandler");
		});
	}
}
