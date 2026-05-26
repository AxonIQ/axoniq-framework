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

import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.Set;

/**
 * Source accumulator for {@link SourcedEventStream#count(Class...) count(Class...)}: a {@link Long}-valued
 * {@link SourcedCondition} that increments a counter for every event whose
 * {@link EventMessage#type() qualified name} belongs to {@code wantedNames}.
 * <p>
 * Type matching is performed entirely on {@link QualifiedName} — the event's payload is never touched.
 * <p>
 * Marked {@link Internal @Internal} because it is the accumulator-side detail behind
 * {@link SourcedEventStream}'s public surface; direct instantiation would skip the type-registration the surrounding
 * stream performs.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class CountCondition extends SourcedCondition<Long> implements NumericCondition<Long> {

    private final Set<QualifiedName> wantedNames;
    private long count;

    CountCondition(SourcedEventStream stream, Set<QualifiedName> wantedNames) {
        super(stream);
        this.wantedNames = Objects.requireNonNull(wantedNames, "wantedNames must not be null");
    }

    @Override
    void accept(EventMessage event) {
        if (wantedNames.contains(event.type().qualifiedName())) {
            count++;
        }
    }

    @Override
    protected Long finalValue() {
        return count;
    }

    @Override
    public Long zero() {
        return 0L;
    }

    @Override
    public Long add(Long a, Long b) {
        return a + b;
    }

    @Override
    public Long subtract(Long a, Long b) {
        return a - b;
    }
}
