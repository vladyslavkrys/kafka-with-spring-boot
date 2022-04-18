package com.learnkafka.libraryeventsproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LibraryEvent {

    // should be generated by the consumer application
    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    @NotNull
    @Valid
    private Book book;

}