/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressContext;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategy;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link SegmentProgressStrategyFactory} that produces a {@link CheckpointingProgressStrategy} for every segment of a
 * processor whose components include one or more self-checkpointing {@link Checkpointing} units. It carries the
 * processor-scoped participants and the auto/deferred mode, and creates one strategy per work package.
 * <p>
 * Use {@link #detecting(String, Supplier)} to obtain the component-driven selector: given a processor's
 * {@link EventHandlingComponent}s it inspects them for {@code Checkpointing} units (via
 * {@link EventHandlingComponent#unwrap(Class)}) and returns either this checkpointing factory (when any participate) or
 * the default {@link SegmentProgressStrategyFactory#tokenStoring() token-storing} factory (when none do).
 * <p>
 * <b>Internal API.</b> This class is marked {@link Internal}: it is part of the self-checkpointing support, intended
 * primarily for internal and advanced use, and its shape may change in a minor or patch release.
 *
 * @author Allard Buijze
 * @see CheckpointingProgressStrategy
 * @since 5.3.0
 */
@Internal
public final class CheckpointingProgressStrategyFactory implements SegmentProgressStrategyFactory {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final List<Checkpointing> participants;
    private final boolean autoCheckpointing;
    private final TokenStore tokenStore;
    private final String processorName;

    /**
     * Constructs a {@code CheckpointingProgressStrategyFactory} for the given {@code participants}, whose strategies
     * extend the claim of their segment through the given {@code tokenStore} for a checkpoint that hands the
     * participants its context but stores no token, see
     * {@link CheckpointingProgressStrategy#CheckpointingProgressStrategy(SegmentProgressContext, List, boolean,
     * TokenStore, String)}.
     *
     * @param participants      the self-checkpointing units of the processor; must not be empty
     * @param autoCheckpointing {@code true} when an ordinary handler is co-located (auto mode), {@code false} when
     *                          every handler is self-checkpointing (fully-deferred)
     * @param tokenStore        the token store of the processor, holding the claims of its segments
     * @param processorName     the name of the processor, under which the token store holds the claims
     */
    public CheckpointingProgressStrategyFactory(List<Checkpointing> participants,
                                                boolean autoCheckpointing,
                                                TokenStore tokenStore,
                                                String processorName) {
        this.participants = List.copyOf(Objects.requireNonNull(participants, "The participants may not be null."));
        if (this.participants.isEmpty()) {
            throw new IllegalArgumentException("The participants may not be empty.");
        }
        this.autoCheckpointing = autoCheckpointing;
        this.tokenStore = Objects.requireNonNull(tokenStore, "The TokenStore may not be null.");
        this.processorName = Objects.requireNonNull(processorName, "The processor name may not be null.");
        EntitlementManager.INSTANCE.registerAddon(CheckpointingAxoniqAddon.class);
    }

    /**
     * Returns the component-driven selector used to pick a {@link SegmentProgressStrategyFactory} for a processor: a
     * {@link CheckpointingProgressStrategyFactory} when its components include self-checkpointing {@link Checkpointing}
     * units, otherwise the default {@link SegmentProgressStrategyFactory#tokenStoring() token-storing} factory.
     * <p>
     * A component is self-checkpointing iff it (or the POJO it wraps) {@link EventHandlingComponent#unwrap(Class)
     * resolves} to a {@code Checkpointing} unit. The processor runs in auto mode unless <em>every</em> component is
     * self-checkpointing (fully-deferred); a mixed processor is logged, since the self-checkpointing components cannot
     * then defer the stored token.
     * <p>
     * The strategies of the checkpointing factory extend the claim of their segment through the token store of the
     * processor for a checkpoint that hands the participants its context but stores no token, so the writes the
     * participants make within that context only commit while this node owns the claim. The token store is obtained
     * from the given {@code tokenStore} supplier once the returned function is applied, which the processor does when
     * it is constructed, so a token store configured after this selector still applies.
     *
     * @param processorName the name of the processor, under which the token store holds the claims of its segments
     * @param tokenStore    supplies the token store of the processor once the processor is constructed
     * @return a function mapping a processor's components to the appropriate progress-strategy factory
     */
    public static Function<List<EventHandlingComponent>, SegmentProgressStrategyFactory> detecting(
            String processorName,
            Supplier<TokenStore> tokenStore
    ) {
        Objects.requireNonNull(processorName, "The processor name may not be null.");
        Objects.requireNonNull(tokenStore, "The TokenStore supplier may not be null.");
        return components -> {
            List<Checkpointing> participants = components.stream()
                                                         .map(c -> c.unwrap(Checkpointing.class))
                                                         .flatMap(Optional::stream)
                                                         .toList();
            if (participants.isEmpty()) {
                return SegmentProgressStrategyFactory.tokenStoring();
            }
            boolean autoCheckpointing = participants.size() < components.size();
            if (autoCheckpointing) {
                // Mixed mode: self-checkpointing components share a processor with ordinary ones. Auto checkpointing
                // wins, so a checkpoint is requested at the batch-end token every batch and the self-checkpointing
                // components are forced to cover it -- they cannot defer the stored token. Log this so the downgrade is
                // not silent.
                logger.info("Detected {} self-checkpointing component(s) alongside ordinary event-handling "
                                    + "component(s); running in auto-checkpointing mode. The self-checkpointing "
                                    + "components cannot defer the stored token and are driven to cover the batch-end "
                                    + "token every batch. Isolate self-checkpointing components in their own processor "
                                    + "to let them control when their segment's token advances.",
                            participants.size());
            }
            return new CheckpointingProgressStrategyFactory(participants, autoCheckpointing, tokenStore.get(),
                                                            processorName);
        };
    }

    /**
     * Returns whether the processor runs fully-deferred: every component is self-checkpointing, so the stored token
     * only advances on their requests (and past skipped events), never at the end of a batch on its own.
     *
     * @return {@code true} when the processor runs fully-deferred, {@code false} when it runs in auto mode
     */
    public boolean isFullyDeferred() {
        return !autoCheckpointing;
    }

    @Override
    public SegmentProgressStrategy create(SegmentProgressContext context) {
        return new CheckpointingProgressStrategy(context, participants, autoCheckpointing, tokenStore, processorName);
    }
}
