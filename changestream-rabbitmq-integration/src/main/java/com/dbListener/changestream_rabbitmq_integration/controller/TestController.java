package com.dbListener.changestream_rabbitmq_integration.controller;

import com.dbListener.changestream_rabbitmq_integration.producer.MessageProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
  private final MessageProducer messageProducer;

  public TestController(MessageProducer messageProducer) {
    this.messageProducer = messageProducer;
  }

  @GetMapping("/send")
  public String sendMessage(@RequestParam String message) {
    messageProducer.sendMessage(message);
    return "Message sent: " + message;
  }
}
