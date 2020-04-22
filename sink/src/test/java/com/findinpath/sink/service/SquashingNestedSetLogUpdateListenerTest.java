package com.findinpath.sink.service;

import com.findinpath.sink.kafka.NestedSetLogUpdatedEvent;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SquashingNestedSetLogUpdateListenerTest {

    private EventBus eventBus;

    @BeforeEach
    public void setup() {
        eventBus = new EventBus();
    }

    @Test
    public void demo() {
        AtomicBoolean eventDispatchedToConsumer = new AtomicBoolean(false);
        Consumer<NestedSetLogUpdatedEvent> consumer = (nestedSetLogUpdatedEvent -> eventDispatchedToConsumer.set(true));
        var squashingNestedSetLogUpdateListener = new SquashingNestedSetLogUpdateListener(consumer,
                eventBus);
        try {

            eventBus.post(new NestedSetLogUpdatedEvent());
            await().atMost(1, TimeUnit.SECONDS).until(eventDispatchedToConsumer::get);
        } finally {
            squashingNestedSetLogUpdateListener.stop();
        }
    }

    @Test
    public void multiThreading() throws Exception {
        AtomicInteger eventsDispatchedToConsumer = new AtomicInteger(0);
        Consumer<NestedSetLogUpdatedEvent> consumer = (nestedSetLogUpdatedEvent -> {
            eventsDispatchedToConsumer.incrementAndGet();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        var squashingNestedSetLogUpdateListener = new SquashingNestedSetLogUpdateListener(consumer,
                eventBus);
        try {

            var nThreads = 20;
            ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

            IntStream.range(0, nThreads)
                    .parallel()
                    .forEach(i -> executorService.execute(() -> eventBus.post(new NestedSetLogUpdatedEvent())));

            // wait until multiple handling cycles of the consumer would have completed.
            Thread.sleep(700);

            var expectedAmountOfEventsDispatchedToConsumer = 2;
            assertThat(eventsDispatchedToConsumer.get(), equalTo(expectedAmountOfEventsDispatchedToConsumer));
        } finally {
            squashingNestedSetLogUpdateListener.stop();
        }
    }
}
