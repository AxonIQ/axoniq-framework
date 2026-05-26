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

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.Set;

/**
 * Source accumulator for {@link SourcedEventStream#contains(Class) contains(Class)} and
 * {@link SourcedEventStream#containsAnyOf(Class...) containsAnyOf(Class...)}: a {@link Boolean}-valued
 * {@link SourcedCondition} that observes the sourced read and flips to {@code true} on the first event whose
 * {@link EventMessage#type() qualified name} belongs to {@code wantedNames}.
 * <p>
 * The single-class {@code contains(...)} path is the degenerate case where {@code wantedNames} carries a single
 * {@link QualifiedName}; the type-set check short-circuits once the predicate has been satisfied, so additional
 * matching events do no extra work.
 * <p>
 * Marked {@link Internal @Internal} because it is the accumulator-side detail behind
 * {@link SourcedEventStream}'s public surface; direct instantiation would skip the type-registration the surrounding
 * stream performs.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class ContainsAnyCondition extends SourcedCondition<Boolean> implements BooleanCondition {

    private final Set<QualifiedName> wantedNames;
    private boolean found;

    ContainsAnyCondition(SourcedEventStream stream, Set<QualifiedName> wantedNames) {
        super(stream);
        this.wantedNames = Objects.requireNonNull(wantedNames, "wantedNames must not be null");
    }

    @Override
    void accept(EventMessage event) {
        if (!found && wantedNames.contains(event.type().qualifiedName())) {
            found = true;
        }
    }

    @Override
    protected Boolean finalValue() {
        return found;
    }
}
