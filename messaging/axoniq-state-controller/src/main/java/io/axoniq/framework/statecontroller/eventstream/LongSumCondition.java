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
import java.util.function.ToLongFunction;

/**
 * Source accumulator for {@link SourcedEventStream#sumLong(Class, ToLongFunction) sumLong(Class, ToLongFunction)}:
 * a {@link Long}-valued {@link SourcedCondition} that adds the primitive {@code long} projected by {@code mapper}
 * for every event whose {@link EventMessage#type() qualified name} equals {@code name}.
 * <p>
 * Uses a primitive {@code long} accumulator internally to avoid the boxing dance every
 * {@link java.math.BigDecimal BigDecimal}-domain alternative would force; the boxed {@link Long} only materializes
 * once when {@link #finalValue()} is invoked on completion. Payload extraction is deferred until a confirmed
 * {@link QualifiedName} match, matching the no-deserialization-for-non-matching-events contract upheld across the
 * source-side accumulators.
 * <p>
 * Marked {@link Internal @Internal} because it is the accumulator-side detail behind
 * {@link SourcedEventStream#sumLong sumLong(...)}; direct instantiation would skip the type-registration the
 * surrounding stream performs.
 *
 * @param <E> the event payload type the {@code mapper} projects from
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class LongSumCondition<E> extends SourcedCondition<Long> implements NumericCondition<Long> {

    private final QualifiedName name;
    private final Class<E> type;
    private final ToLongFunction<? super E> mapper;
    private long total;

    LongSumCondition(SourcedEventStream stream,
                     QualifiedName name,
                     Class<E> type,
                     ToLongFunction<? super E> mapper) {
        super(stream);
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    }

    @Override
    void accept(EventMessage event) {
        if (event.type().qualifiedName().equals(name)) {
            E payload = event.payloadAs(type, stream.converter());
            total += mapper.applyAsLong(payload);
        }
    }

    @Override
    protected Long finalValue() {
        return total;
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
