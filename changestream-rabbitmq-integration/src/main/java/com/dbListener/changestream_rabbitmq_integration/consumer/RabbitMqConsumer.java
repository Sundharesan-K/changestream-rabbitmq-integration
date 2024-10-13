package com.dbListener.changestream_rabbitmq_integration.consumer;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqConsumer {

  private static final Logger log = LoggerFactory.getLogger(RabbitMqConsumer.class);

  @RabbitListener(queues = RabbitMqConfig.QUEUE_NAME, containerFactory = "rabbitListenerContainerFactory")
  public void receiveMessage(String message, Channel channel,
   @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
    try {
      log.info("Received message : {}", message);
      Thread.sleep(5000);
      channel.basicAck(deliveryTag, false);
      log.info("Acknowledged message with delivery tag: {}", deliveryTag);
    }catch (Exception e){
      log.error("Error processing message : {}", e.getMessage());
    }
  }
}
