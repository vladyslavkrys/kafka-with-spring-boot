package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;
    @SpyBean // access to the real bean
    LibraryEventsConsumer libraryEventsConsumerSpy;
    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;
    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            // make sure that container LibraryEventsConsumer is going to wait until all the partitions assigned to it
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void shouldPublishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\": \"NEW\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 2.X.0\",\n" +
                "        \"bookAuthor\": \"Vlad\"\n" +
                "    }\n" +
                "}";

        kafkaTemplate.sendDefault(json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        boolean await = latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertEquals(libraryEvents.size(), 1);
        libraryEvents.forEach(libraryEvent -> {
            assertNotNull(libraryEvent.getLibraryEventId());
            assertEquals(456, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void shouldPublishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\": \"NEW\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 2.X.0\",\n" +
                "        \"bookAuthor\": \"Vlad\"\n" +
                "    }\n" +
                "}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot 2.X.0")
                .bookAuthor("Vlad")
                .build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        boolean await = latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.X.0", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void shouldThrowExceptionWhenLibraryEventIdNull() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        Integer libraryEventId = null;
        String json = "{\n" +
                "    \"libraryEventId\": " + libraryEventId + ",\n" +
                "    \"libraryEventType\": \"UPDATE\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 2.X.0\",\n" +
                "        \"bookAuthor\": \"Vlad\"\n" +
                "    }\n" +
                "}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        boolean await = latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(6)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(6)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void shouldThrowExceptionWhenLibraryEventIdIsNotExist() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        Integer libraryEventId = 123;
        String json = "{\n" +
                "    \"libraryEventId\": " + libraryEventId + ",\n" +
                "    \"libraryEventType\": \"UPDATE\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 2.X.0\",\n" +
                "        \"bookAuthor\": \"Vlad\"\n" +
                "    }\n" +
                "}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        boolean await = latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(4)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());
    }

    @Test
    void shouldThrowExceptionWhenLibraryEventIdIsZero() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        // 0 - test exception for the retries
        Integer libraryEventId = 0;
        String json = "{\n" +
                "    \"libraryEventId\": " + libraryEventId + ",\n" +
                "    \"libraryEventType\": \"UPDATE\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 2.X.0\",\n" +
                "        \"bookAuthor\": \"Vlad\"\n" +
                "    }\n" +
                "}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        boolean await = latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());
    }
}
