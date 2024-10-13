package com.dbListener.changestream_rabbitmq_integration.changeStream;

import com.dbListener.changestream_rabbitmq_integration.producer.MessageProducer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import jakarta.annotation.PostConstruct;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class MongoChangeStreamListener {

  private final MessageProducer producer;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @PostConstruct
  public void init() {
    executorService.submit(this::startChangeStream);
  }

  private void startChangeStream() {
    try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
      MongoDatabase database = client.getDatabase("bank_application");

      log.info("Watching changes on the entire 'bank_application' database.");

      // Watch changes on all collections in the database
      database.watch()
          .fullDocument(FullDocument.UPDATE_LOOKUP)
          .forEach(this::handleChange);
    } catch (Exception e) {
      log.error("Error in database-level change stream: {}", e.getMessage());
    }
  }

  private void handleChange(ChangeStreamDocument<Document> change) {
    String collectionName = Objects.requireNonNull(change.getNamespace()).getCollectionName();
    Document fullDocument = change.getFullDocument();
    if (Objects.nonNull(fullDocument)) {
      log.info("Operation: {}, Data: {}", change.getOperationType(), fullDocument.toJson());
      String message = "Operation: " + change.getOperationType() + ", Data: " + fullDocument.toJson();
      producer.sendMessage(message);
      log.info("Change in collection '{}': {} - Data: {}",
          collectionName, change.getOperationType(), fullDocument.toJson());
    }
  }
}
