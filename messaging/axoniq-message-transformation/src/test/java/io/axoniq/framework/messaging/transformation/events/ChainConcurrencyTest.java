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

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The chain is safe to invoke concurrently. Eight workers, started simultaneously via a
 * {@link CountDownLatch} starting gun, each runs ten thousand chain invocations against
 * the same shared chain instance and the same input event. Any divergence in output type
 * or payload content fails the test with the iteration index of the first failure.
 */
final class ChainConcurrencyTest {

    private static final int THREADS = 8;
    private static final int ITERATIONS_PER_THREAD = 10_000;

    /** Sentinel for "no worker has recorded a failure yet". Any non-negative value is a real iteration index. */
    private static final int NO_FAILURE_RECORDED = -1;

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentInvocationsProduceIdenticalOutputs() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1).to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        ObjectNode stablePayload = JsonNodeFactory.instance.objectNode().put("k", "v");
        EventMessage stableInput = new GenericEventMessage(V1, stablePayload);

        CountDownLatch startingGun = new CountDownLatch(1);
        AtomicInteger firstFailedIteration = new AtomicInteger(NO_FAILURE_RECORDED);

        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<CompletableFuture<Void>> workers = IntStream.range(0, THREADS)
                    .mapToObj(threadIndex -> CompletableFuture.runAsync(() -> {
                        awaitStart(startingGun);
                        runIterations(chain, stableInput, stablePayload, firstFailedIteration);
                    }, pool))
                    .toList();

            startingGun.countDown();
            workers.forEach(CompletableFuture::join);
        }

        assertThat(firstFailedIteration.get())
                .as("expected no worker iteration to fail; value below is the index of the failing iteration")
                .isEqualTo(NO_FAILURE_RECORDED);
    }

    private static void awaitStart(CountDownLatch startingGun) {
        try {
            startingGun.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("worker interrupted before start", interrupted);
        }
    }

    private static void runIterations(EventTransformerChain chain,
                                      EventMessage input,
                                      JsonNode expectedPayload,
                                      AtomicInteger firstFailedIteration) {
        for (int iteration = 0; iteration < ITERATIONS_PER_THREAD; iteration++) {
            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(input))));
            if (outputs.size() != 1
                    || !V2.equals(outputs.getFirst().type())
                    || !expectedPayload.equals(outputs.getFirst().payload())) {
                firstFailedIteration.compareAndSet(NO_FAILURE_RECORDED, iteration);
                return;
            }
        }
    }
}
