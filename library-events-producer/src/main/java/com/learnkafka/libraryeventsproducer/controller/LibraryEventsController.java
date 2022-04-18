package com.learnkafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.domain.LibraryEventType;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> saveLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        // invoke kafka producer
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventAsyncUsingSend(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    // PUT
    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        if (libraryEvent.getLibraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event id.");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEventAsyncUsingSend(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
