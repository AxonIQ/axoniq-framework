/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.kafka.publishing;

import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.license.entitlement.EntitlementManager;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

/**
 * Publishes records in Kafka transactions, for the {@link ConfirmationMode#TRANSACTIONAL} mode of a
 * {@link KafkaEventSink}.
 * <p>
 * The records published within one processing context go into one Kafka transaction, which commits while the context
 * prepares to commit, and aborts when the context fails before that. Records published outside a processing context
 * get a transaction of their own, which commits right away. Each transaction runs on a producer borrowed from a
 * {@link ProducerPool} that this publisher owns; the producer goes back to the pool, or is discarded, exactly once,
 * when the transaction commits or aborts.
 * <p>
 * Internal, because it is created by and only reachable through {@link KafkaEventSink}.
 *
 * @param <V> the type of record value
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
class TransactionalPublisher<V> {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalPublisher.class);

    private final ProducerPool<V> pool;
    private final @Nullable Executor commitExecutor;
    private final EntitlementManager entitlementManager;
    private final ResourceKey<ContextTransaction> transactionKey = ResourceKey.withLabel("kafkaEventSinkTransaction");
    // Set when the owning sink shuts down, before the pool closes, so that no new transaction begins from then on.
    private volatile boolean closed;

    /**
     * Constructs a publisher whose pool creates transactional producers with the given {@code clientFactory},
     * {@code producerProperties} and {@code valueSerializer}, with transactional identifiers starting with the given
     * {@code transactionalIdPrefix}.
     *
     * @param clientFactory         the factory creating the producers
     * @param producerProperties    the settings of the producers
     * @param valueSerializer       the serializer of the record values
     * @param transactionalIdPrefix the prefix of the producers' transactional identifiers
     * @param commitExecutor        the executor committing transactions, or {@code null} to commit on the thread that
     *                              commits the processing context
     * @param entitlementManager    the entitlement manager the events are claimed against once their transaction
     *                              committed
     */
    TransactionalPublisher(KafkaClientFactory clientFactory,
                           Map<String, Object> producerProperties,
                           Serializer<V> valueSerializer,
                           String transactionalIdPrefix,
                           @Nullable Executor commitExecutor,
                           EntitlementManager entitlementManager) {
        this.pool = new ProducerPool<>(clientFactory, producerProperties, valueSerializer, transactionalIdPrefix);
        this.commitExecutor = commitExecutor;
        this.entitlementManager = entitlementManager;
    }

    /**
     * Sends the given {@code records} in the Kafka transaction of the given {@code context}, beginning it when these
     * are the context's first records. The transaction commits while the context prepares to commit.
     *
     * @param context the processing context the records are published in
     * @param records the records to send
     * @throws IllegalStateException     if the context is already committing, or its transaction finished
     * @throws KafkaPublicationException if beginning the transaction or sending a record failed
     */
    void send(ProcessingContext context, List<ProducerRecord<String, V>> records) {
        ContextTransaction transaction = context.computeResourceIfAbsent(transactionKey, () -> {
            ContextTransaction created = new ContextTransaction();
            // Registered before a producer is borrowed, so that a publish too late in the context leaks nothing.
            PublishingSupport.registerLate(() -> context.onPrepareCommit(c -> created.commit()));
            context.onError((c, phase, error) -> created.abort());
            return created;
        });
        transaction.send(records);
    }

    /**
     * Sends the given {@code records} in a Kafka transaction of their own, and commits it.
     *
     * @param records the records to send
     * @return a future that completes when the transaction committed
     * @throws KafkaPublicationException if beginning the transaction or sending a record failed
     */
    CompletableFuture<Void> publishOutsideContext(List<ProducerRecord<String, V>> records) {
        ContextTransaction transaction = new ContextTransaction();
        try {
            transaction.send(records);
        } catch (RuntimeException e) {
            transaction.abort();
            throw e;
        }
        return transaction.commit();
    }

    /**
     * Rejects beginning new transactions from now on, so that publishing in a processing context without a transaction
     * yet, or outside a processing context, fails with an {@link IllegalStateException}. Transactions that already
     * began can still send and commit.
     */
    void rejectTransactions() {
        closed = true;
    }

    /**
     * Rejects new transactions and closes the pool's producers, giving them at most the given {@code timeout} together
     * to send what they have buffered. A producer that is in use is closed once its transaction ends.
     *
     * @param timeout the longest time closing the producers takes
     */
    void close(Duration timeout) {
        closed = true;
        pool.close(timeout);
    }

    /**
     * The Kafka transaction of one processing context, running on a producer borrowed from the pool when the first
     * records are sent. The producer goes back to the pool, or is discarded, exactly once: when the transaction commits
     * or aborts. A finished transaction rejects further sends.
     */
    private class ContextTransaction {

        private @Nullable Producer<String, V> producer;
        private boolean finished;
        private boolean beginFailed;
        private volatile @Nullable Throwable failure;
        private int count;

        private synchronized void send(List<ProducerRecord<String, V>> records) {
            if (finished) {
                throw new IllegalStateException(
                        beginFailed
                                ? "Events can no longer be published to Kafka in this processing context: its Kafka "
                                + "transaction could not be begun."
                                : "Events can no longer be published to Kafka in this processing context: its Kafka "
                                + "transaction has already been committed or aborted."
                );
            }
            Producer<String, V> current = producer != null ? producer : begin();
            for (ProducerRecord<String, V> record : records) {
                PublishingSupport.send(current, record).whenComplete((result, error) -> {
                    if (error != null) {
                        failure = error;
                    }
                });
                Throwable failed = failure;
                if (failed != null) {
                    throw failed instanceof KafkaPublicationException publicationException
                            ? publicationException
                            : PublishingSupport.publicationFailure(record, failed);
                }
            }
            count += records.size();
        }

        private Producer<String, V> begin() {
            if (closed) {
                throw PublishingSupport.sinkShutDown();
            }
            Producer<String, V> borrowed;
            try {
                // Creates a producer and initializes its transactions when none is idle.
                borrowed = pool.borrow();
            } catch (RuntimeException e) {
                if (closed) {
                    throw PublishingSupport.sinkShutDown();
                }
                throw new KafkaPublicationException("Obtaining a transactional Kafka producer failed.", e);
            }
            try {
                borrowed.beginTransaction();
            } catch (RuntimeException e) {
                finished = true;
                beginFailed = true;
                pool.discard(borrowed);
                throw new KafkaPublicationException("Beginning a Kafka transaction failed.", e);
            }
            producer = borrowed;
            return borrowed;
        }

        /**
         * Commits the transaction. Only the first call to commit or abort takes the producer: it alone returns the
         * producer to the pool or discards it, and every later send is rejected.
         */
        private CompletableFuture<Void> commit() {
            Producer<String, V> current;
            int committed;
            synchronized (this) {
                if (finished) {
                    return FutureUtils.emptyCompletedFuture();
                }
                finished = true;
                current = producer;
                committed = count;
            }
            if (current == null) {
                return FutureUtils.emptyCompletedFuture();
            }
            Executor executor = commitExecutor;
            if (executor == null) {
                // Blocks the thread that commits the processing context, for at most the producer's max.block.ms.
                // Acceptable as the default: the context cannot complete its commit before Kafka has answered anyway,
                // and the processor's thread waits for that context. A commit executor frees the thread.
                return commit(current, committed);
            }
            try {
                return CompletableFuture.supplyAsync(() -> commit(current, committed), executor)
                                        .thenCompose(Function.identity());
            } catch (RejectedExecutionException e) {
                abortAndReturn(current);
                return CompletableFuture.failedFuture(
                        new KafkaPublicationException("Committing the Kafka transaction could not be scheduled.", e)
                );
            }
        }

        private CompletableFuture<Void> commit(Producer<String, V> current, int committed) {
            try {
                Throwable failed = failure;
                if (failed != null) {
                    throw failed instanceof KafkaPublicationException publicationException
                            ? publicationException
                            : new KafkaPublicationException("Publishing events to Kafka failed.", failed);
                }
                current.commitTransaction();
            } catch (ProducerFencedException e) {
                logger.warn("The Kafka producer was fenced off by another producer with the same transactional "
                                    + "identifier. Make sure each application instance uses a transactional identifier "
                                    + "prefix of its own.", e);
                pool.discard(current);
                return CompletableFuture.failedFuture(
                        new KafkaPublicationException("Committing the Kafka transaction failed.", e)
                );
            } catch (org.apache.kafka.common.errors.TimeoutException | InterruptException e) {
                // The transaction may still commit on the broker. Kafka allows only retrying the commit or closing the
                // producer, not aborting, so the producer is discarded.
                logger.warn("Committing a Kafka transaction did not complete. The transaction may still commit, so its "
                                    + "events may still be published.", e);
                pool.discard(current);
                return CompletableFuture.failedFuture(
                        new KafkaPublicationException("Committing the Kafka transaction did not complete.", e)
                );
            } catch (KafkaPublicationException e) {
                abortAndReturn(current);
                return CompletableFuture.failedFuture(e);
            } catch (RuntimeException e) {
                abortAndReturn(current);
                return CompletableFuture.failedFuture(
                        new KafkaPublicationException("Committing the Kafka transaction failed.", e)
                );
            }
            // Outside the try: once released, the producer belongs to the pool, and nothing may abort it any more.
            pool.release(current);
            PublishingSupport.claim(entitlementManager, committed);
            return FutureUtils.emptyCompletedFuture();
        }

        private synchronized void abort() {
            if (!finished) {
                finished = true;
                if (producer != null) {
                    abortAndReturn(producer);
                }
            }
        }

        private void abortAndReturn(Producer<String, V> current) {
            try {
                current.abortTransaction();
                pool.release(current);
            } catch (RuntimeException e) {
                logger.debug("Aborting a Kafka transaction failed; the producer is discarded.", e);
                pool.discard(current);
            }
        }
    }
}
