package org.synyx.deliverycountexample;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

import static org.synyx.deliverycountexample.RabbitMqConfig.EXAMPLE_QUEUE;

@Component
@Slf4j
public class ExampleRabbitListener {

    private static final AtomicInteger ATTEMPT_COUNTER = new AtomicInteger();

    public static void resetAttempts() {
        ATTEMPT_COUNTER.set(0);
    }

    public static int attempts() {
        return ATTEMPT_COUNTER.get();
    }

    @RabbitListener(
            queues = EXAMPLE_QUEUE)
    public void receiveTask(Message message) {
        ATTEMPT_COUNTER.incrementAndGet();
        log.info("Received message (x-acquired-count={}, x-delivery-count={})", message.getMessageProperties().getHeaders().get("x-acquired-count"), message.getMessageProperties().getHeaders().get("x-delivery-count"));
        throw new UnsupportedOperationException("Boom! This task always fails.");
    }
}
