package com.example.kafkacache.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@Slf4j
@Component
public class MyWebSocketHandler implements WebSocketHandler {

    @Override
    public Mono<Void> handle(WebSocketSession session) {

        final var sink = Sinks.<String>one();
        final var disposable = session.receive()
                .doOnNext(WebSocketMessage::retain)
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(msg -> System.out.println("Received message: " + msg))
                .log()
                .subscribe(sink::tryEmitValue, err -> System.out.println("Error occurred: " + err.getMessage()));

        final var responseMono = sink.asMono()
                .flatMapMany(msg -> {
                    System.out.println("Build response...");
                    return Flux.merge(5000, Flux.concat(Mono.just("first"), this.mockMessages()));
                })
                .map(session::textMessage)
                .doOnError(err -> System.out.println("Error occurred: " + err.getMessage()))
                .doOnCancel(() -> System.out.println("WebSocket session cancelled"))
                .doFinally(signalType -> System.out.println("WebSocket session closed"))
                .log();

        return session.send(responseMono)
                .doOnTerminate(() -> {
                    System.out.println("WebSocket session terminated");
                    disposable.dispose();
                });
    }


    private Flux<String> mockMessages() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(i -> "Message " + i);
    }
}
