package org.synyx.deliverycountexample;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ErrorHandler;

@Configuration
@Slf4j
public class RabbitMqConfig {

    public static final String TOPIC_EXCHANGE_NAME = "example-topic";
    public static final String EXAMPLE_TASK_ROUTING_KEY = "example.task";
    public static final String EXAMPLE_QUEUE = "example-queue";
    public static final int NUMBER_OF_RETRIES = 4; // 4 retries, 5 attempts in total´

    public static final String DEAD_LETTER_EXCHANGE = "dead-letter-topic";
    public static final String DEAD_LETTER_KEY = "example.dead";
    public static final String DEAD_LETTER_QUEUE = "dead_letter";

    @Bean
    TopicExchange exampleTopicExchange() {
        ExchangeBuilder exchangeBuilder =
                ExchangeBuilder.topicExchange(TOPIC_EXCHANGE_NAME);
        return exchangeBuilder.build();
    }

    @Bean
    Queue exampleTaskQueue() {
        return QueueBuilder.durable(EXAMPLE_QUEUE)
                .quorum()
                .deadLetterExchange(DEAD_LETTER_EXCHANGE)
                .deadLetterRoutingKey(DEAD_LETTER_KEY)
                .deliveryLimit(NUMBER_OF_RETRIES)
                .build();
    }

    @Bean
    Binding exampleTaskQueueBinding(
            Queue exampleTaskQueue, TopicExchange exampleTopicExchange) {
        return BindingBuilder.bind(exampleTaskQueue)
                .to(exampleTopicExchange)
                .with(EXAMPLE_TASK_ROUTING_KEY);
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return ExchangeBuilder.directExchange(DEAD_LETTER_EXCHANGE).build();
    }

    @Bean
    Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE)
                .quorum()
                .build();
    }

    @Bean
    Binding deadLetterBinding(Queue deadLetterQueue, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(DEAD_LETTER_KEY);
    }

    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setErrorHandler(rabbitListenerErrorHandler());
        return factory;
    }

    private ErrorHandler rabbitListenerErrorHandler() {
        ConditionalRejectingErrorHandler delegate = new ConditionalRejectingErrorHandler();
        return throwable -> {
            if (throwable instanceof ListenerExecutionFailedException failed
                    && failed.getFailedMessage() != null) {
                MessageProperties props = failed.getFailedMessage().getMessageProperties();
                Object acquiredCount = props.getHeaders().get("x-acquired-count");
                Object deliveryCount = props.getHeaders().get("x-delivery-count");
                log.error(
                        "Rabbit listener failed; queue={}, routingKey={}, x-acquired-count={}, x-delivery-count={}",
                        props.getConsumerQueue(),
                        props.getReceivedRoutingKey(),
                        acquiredCount,
                        deliveryCount,
                        throwable);
            }
            delegate.handleError(throwable);
        };
    }
}
