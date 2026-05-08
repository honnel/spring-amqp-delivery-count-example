package org.synyx.deliverycountexample;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RabbitRetryIT {

    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);

    @ParameterizedTest
    @ValueSource(strings = {"rabbitmq:4.2.5-management", "rabbitmq:4.3.0-management"})
    void retriesFourTimesAndDeadLettersAfterFifthProcessingAttempt(String rabbitImage) {
        try (RabbitMQContainer rabbitMq = new RabbitMQContainer(DockerImageName.parse(rabbitImage))) {
            rabbitMq.start();

            try (ConfigurableApplicationContext context =
                         new SpringApplicationBuilder(DeliveryCountExampleApplication.class)
                                 .properties(
                                         "spring.docker.compose.enabled=false",
                                         "spring.main.web-application-type=none",
                                         "spring.rabbitmq.host=" + rabbitMq.getHost(),
                                         "spring.rabbitmq.port=" + rabbitMq.getAmqpPort(),
                                         "spring.rabbitmq.username=" + rabbitMq.getAdminUsername(),
                                         "spring.rabbitmq.password=" + rabbitMq.getAdminPassword())
                                 .run()) {
                RabbitTemplate rabbitTemplate = context.getBean(RabbitTemplate.class);

                rabbitTemplate.execute(channel -> {
                    channel.queuePurge(RabbitMqConfig.EXAMPLE_QUEUE);
                    channel.queuePurge(RabbitMqConfig.DEAD_LETTER_QUEUE);
                    return null;
                });
                ExampleRabbitListener.resetAttempts();

                rabbitTemplate.convertAndSend(
                        RabbitMqConfig.TOPIC_EXCHANGE_NAME,
                        RabbitMqConfig.EXAMPLE_TASK_ROUTING_KEY,
                        "test");

                await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                        assertThat(ExampleRabbitListener.attempts())
                                .isEqualTo(RabbitMqConfig.NUMBER_OF_RETRIES + 1));

                Message deadLetterMessage = await().atMost(WAIT_TIMEOUT)
                        .until(
                                () -> rabbitTemplate.receive(RabbitMqConfig.DEAD_LETTER_QUEUE),
                                Objects::nonNull);

                assertThat(deadLetterMessage).isNotNull();
                assertThat(extractFirstXDeathReason(deadLetterMessage)).isEqualTo("delivery_limit");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private String extractFirstXDeathReason(Message deadLetterMessage) {
        Object xDeathHeader = deadLetterMessage.getMessageProperties().getHeaders().get("x-death");
        assertThat(xDeathHeader).isInstanceOf(List.class);

        List<Map<String, Object>> xDeathEntries = (List<Map<String, Object>>) xDeathHeader;
        assertThat(xDeathEntries).isNotEmpty();

        return Objects.toString(xDeathEntries.getFirst().get("reason"), null);
    }
}

