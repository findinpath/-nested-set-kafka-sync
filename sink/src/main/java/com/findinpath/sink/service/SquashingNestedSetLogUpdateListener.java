package com.findinpath.sink.service;

import com.findinpath.sink.kafka.NestedSetLogUpdatedEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * This update listener gets notified whenever new entries are written to the
 * database table <code>nested_set_node_log</code> and it delegates towards the
 * consumer in an asynchronous fashion the responsibility of
 * trying to merge the newest updates into the <code>nested_set_node</code> table.
 * <p>
 * The class makes use of an {@link Executor} for dispatching the calls asynchronously towards
 * consumer. When using a single threaded executor, in case of issuing fast <code>NestedSetLogUpdatedEvent</code>
 * they will be enqueued, but they'll be handled sequentially.
 * When using an executor having <code>3</code> threads there can be achieved the squashing of the
 * <code>NestedSetLogUpdatedEvent</code> events. In case that an event is already enqueued for processing and
 * another <code>NestedSetLogUpdatedEvent</code> event is being received, the new event will not be enqueued anymore
 * leading to less database work. As mentioned before at least <code>3</code> threads would be enough because:
 * <ul>
 *     <li>one of the threads would be busy doing the consumption of the event</li>
 *     <li>another thread would be busy trying to acquire the lock for doing consumption of the event</li>
 *     <li>this thread would verify if there is an event already enqueued, and if so it would complete the method call.</li>
 * </ul>
 */
public class SquashingNestedSetLogUpdateListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SquashingNestedSetLogUpdateListener.class);

    private final Consumer<NestedSetLogUpdatedEvent> consumer;
    private Executor notificationExecutor;

    private AtomicBoolean notificationEnqueued = new AtomicBoolean(false);
    private ReentrantReadWriteLock notificationLock = new ReentrantReadWriteLock();


    /**
     * Constructor of the class.
     *
     * @param consumer             gets notified (in an async fashion) about updates in the nested set log
     * @param notificationExecutor the executor used by this listener for notifying
     *                             the <code>nestedSyncService</code> about updates in the nested set log
     */
    public SquashingNestedSetLogUpdateListener(Consumer<NestedSetLogUpdatedEvent> consumer,
                                               Executor notificationExecutor,
                                               EventBus eventBus) {
        this.consumer = consumer;
        this.notificationExecutor = notificationExecutor;
        eventBus.register(this);
    }

    @Subscribe
    public void onNestedSetLogUpdated(NestedSetLogUpdatedEvent nestedSetLogUpdatedEvent) {
        LOGGER.info("Received NestedSetLogUpdatedEvent");
        notificationExecutor.execute(() -> this.notifySyncService(nestedSetLogUpdatedEvent));
    }

    private void notifySyncService(NestedSetLogUpdatedEvent nestedSetLogUpdatedEvent) {
        var isNotificationEnqueued = notificationEnqueued.compareAndSet(false, true);

        if (isNotificationEnqueued) {
            try {
                while (!notificationLock.writeLock().tryLock()) {
                }
                notificationEnqueued.set(false);

                LOGGER.info("Notifying nestedSetSyncService about new updates on the nested_set_node_log table");
                consumer.accept(nestedSetLogUpdatedEvent);

            } finally {
                notificationLock.writeLock().unlock();
            }
        }

    }
}
