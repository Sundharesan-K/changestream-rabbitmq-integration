package com.dbListener.changestream_rabbitmq_integration.producer;

import static com.dbListener.changestream_rabbitmq_integration.consumer.RabbitMqConfig.EXCHANGE_NAME;
import static com.dbListener.changestream_rabbitmq_integration.consumer.RabbitMqConfig.QUEUE_NAME;
import static com.dbListener.changestream_rabbitmq_integration.consumer.RabbitMqConfig.ROUTING_KEY;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageProducer {

  private final RabbitTemplate rabbitTemplate;

  public void sendMessage(String message) {
    rabbitTemplate.convertAndSend(EXCHANGE_NAME, ROUTING_KEY, message);
    log.info("Message sent : {}", message);
  }
}
