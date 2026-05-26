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

package io.axoniq.framework.statecontroller.eventstream;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.Set;

/**
 * Source-side accumulator that tracks a single {@link EventMessage} from the sourced read — either the latest
 * or the first event whose {@link EventMessage#type() type} appears in {@code wantedNames}.
 * <p>
 * Unlike payload-extracting accumulators, this accumulator never invokes
 * {@link EventMessage#payload() payload()}. Its value-future carries {@link Optional Optional&lt;EventMessage&gt;},
 * which lets downstream views derive type-only projections ({@code isA}, {@code isAnyOf}, {@code isNamed}) and
 * deferred payload extraction ({@code as}, {@code matching}) without forcing deserialization of non-matching
 * events.
 * <p>
 * Marked {@link Internal @Internal} because it is the source half of the {@link SourcedEventCondition} pair;
 * direct instantiation would skip the eager type-registration performed by {@link SourcedEventStream}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class SourcedEventSelection extends SourcedCondition<Optional<EventMessage>> {

    /**
     * Selection strategy. The match set is checked against the event's qualified name; on a hit, the event
     * either becomes the latest match (overwriting any previous match) or the first match (only set if no
     * prior match exists).
     */
    enum Mode {LATEST, FIRST}

    private final Set<QualifiedName> wantedNames;
    private final Mode mode;
    private @Nullable EventMessage match;

    private SourcedEventSelection(SourcedEventStream stream, Set<QualifiedName> wantedNames, Mode mode) {
        super(stream);
        this.wantedNames = Set.copyOf(wantedNames);
        this.mode = mode;
    }

    static SourcedEventSelection latestOf(SourcedEventStream stream, Set<QualifiedName> wantedNames) {
        return new SourcedEventSelection(stream, wantedNames, Mode.LATEST);
    }

    static SourcedEventSelection firstOf(SourcedEventStream stream, Set<QualifiedName> wantedNames) {
        return new SourcedEventSelection(stream, wantedNames, Mode.FIRST);
    }

    @Override
    void accept(EventMessage event) {
        if (!wantedNames.contains(event.type().qualifiedName())) {
            return;
        }
        if (mode == Mode.LATEST || match == null) {
            match = event;
        }
    }

    @Override
    protected Optional<EventMessage> finalValue() {
        return Optional.ofNullable(match);
    }
}
