package com.dbListener.changestream_rabbitmq_integration.consumer;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

  public static final String QUEUE_NAME = "rabbitMq01Queue";
  public static final String EXCHANGE_NAME = "rabbitMq01Exchange";
  public static final String ROUTING_KEY = "rabbitMq01RoutingKey";

  @Bean
  public Queue queue() {
    return new Queue(QUEUE_NAME, true);
  }

  @Bean
  public TopicExchange exchange() {
    return new TopicExchange(EXCHANGE_NAME);
  }

  @Bean
  public Binding binding(Queue queue, TopicExchange exchange) {
    return BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY);
  }

  @Bean
  public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
      ConnectionFactory connectionFactory) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setDefaultRequeueRejected(false);
    factory.setConcurrentConsumers(2);
    factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    factory.setPrefetchCount(1);
    return factory;
  }
}
