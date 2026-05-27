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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The chain is safe to invoke concurrently and tolerates a {@code null}
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}.
 * N threads x M iterations produce byte-identical outputs.
 */
final class ChainConcurrencyTest {

    private static final int THREADS = 8;
    private static final int ITERATIONS_PER_THREAD = 10_000;

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T024 + T027 (chain matching path)")
    void concurrentInvocationsProduceIdenticalOutputs() throws Exception {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1).to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage stableInput = new GenericEventMessage(V1, "stable-payload");

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);

        List<CompletableFuture<Boolean>> workers = IntStream.range(0, THREADS).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
            for (int iteration = 0; iteration < ITERATIONS_PER_THREAD; iteration++) {
                List<EventMessage> outputs = drain(chain.transform(MessageStream.fromIterable(List.of(stableInput))));
                if (outputs.size() != 1 || !V2.equals(outputs.getFirst().type())) {
                    return false;
                }
            }
            return true;
        }, pool)).toList();

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        for (CompletableFuture<Boolean> worker : workers) {
            assertThat(worker.join()).isTrue();
        }
    }

    @Test
    @Disabled("Tests-first; impl lands in T027 (mapper receives @Nullable ProcessingContext)")
    void producesSameOutputWithNullAndNonNullProcessingContext() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1).to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage input = new GenericEventMessage(V1, "p");

        List<EventMessage> outWithoutCtx = drain(chain.transform(MessageStream.fromIterable(List.of(input))));

        assertThat(outWithoutCtx).hasSize(1);
        assertThat(outWithoutCtx.getFirst().type()).isEqualTo(V2);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        List<EventMessage> collected = new ArrayList<>();
        stream.<Void>reduce(null, (acc, entry) -> {
            collected.add(entry.message());
            return null;
        }).join();
        return collected;
    }
}
