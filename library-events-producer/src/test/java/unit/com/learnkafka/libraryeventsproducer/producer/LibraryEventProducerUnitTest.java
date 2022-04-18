package unit.com.learnkafka.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.Book;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    private static final String LIBRARY_EVENTS = "library-events";

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;
    // @Spy will wrap an existing instance.
    // It will still behave in the same way as the normal instance,
    // the only difference is that it will also be instrumented to track all the interactions with it.
    @Spy
    ObjectMapper objectMapper;

    @InjectMocks
    LibraryEventProducer eventProducer;

    @Test
    void shouldSendLibraryEventAsyncUsingSendAndCallbackWithFailure() {
        // given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Vlad")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture<Exception> future = new SettableListenableFuture<>();

        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // when
        assertThrows(Exception.class, () -> eventProducer.sendLibraryEventAsyncUsingSend(libraryEvent).get());
    }

    @Test
    void shouldSendLibraryEventAsyncUsingSendAndCallbackWithSuccess() throws JsonProcessingException,
            ExecutionException, InterruptedException {
        // given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Vlad")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<>();

        SendResult<Integer, String> sendResult = createSendResult(libraryEvent);
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // when
        ListenableFuture<SendResult<Integer, String>> listenableFuture = eventProducer.sendLibraryEventAsyncUsingSend(libraryEvent);

        // then
        SendResult<Integer, String> returnedSendResult = listenableFuture.get();
        assert returnedSendResult.getRecordMetadata().partition() == 1;
    }

    private SendResult<Integer, String> createSendResult(LibraryEvent libraryEvent) throws JsonProcessingException {
        String jsonRecord = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(LIBRARY_EVENTS, libraryEvent.getLibraryEventId(), jsonRecord);
        TopicPartition topicPartition = new TopicPartition(LIBRARY_EVENTS, 1);
        RecordMetadata recordMetadata = new RecordMetadata(topicPartition, 1, 1, 342, System.currentTimeMillis(), 1, 2);
        return new SendResult<>(producerRecord, recordMetadata);
    }
}
