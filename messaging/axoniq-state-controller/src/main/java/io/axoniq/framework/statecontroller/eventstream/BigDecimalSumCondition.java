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

import java.math.BigDecimal;
import java.util.Objects;
import java.util.function.Function;

/**
 * Source accumulator for {@link SourcedEventStream#sum(Class, Function) sum(Class, Function)}: a
 * {@link BigDecimal}-valued {@link SourcedCondition} that adds the {@code mapper}-projected value of every event
 * whose {@link EventMessage#type() qualified name} equals {@code name}.
 * <p>
 * Payload extraction is performed only on a confirmed {@link QualifiedName} match — non-matching events do not
 * trigger {@link EventMessage#payload() payload()} or {@link EventMessage#payloadAs(Class,
 * org.axonframework.conversion.Converter) payloadAs(...)}.
 * <p>
 * Marked {@link Internal @Internal} because it is the accumulator-side detail behind
 * {@link SourcedEventStream#sum sum(...)}; direct instantiation would skip the type-registration the surrounding
 * stream performs.
 *
 * @param <E> the event payload type the {@code mapper} projects from
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class BigDecimalSumCondition<E> extends SourcedCondition<BigDecimal> implements NumericCondition<BigDecimal> {

    private final QualifiedName name;
    private final Class<E> type;
    private final Function<? super E, BigDecimal> mapper;
    private BigDecimal total = BigDecimal.ZERO;

    BigDecimalSumCondition(SourcedEventStream stream,
                           QualifiedName name,
                           Class<E> type,
                           Function<? super E, BigDecimal> mapper) {
        super(stream);
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    }

    @Override
    void accept(EventMessage event) {
        if (event.type().qualifiedName().equals(name)) {
            // Payload is extracted only on a confirmed type match. payloadAs(type, converter) short-circuits to
            // a direct cast when the payload is already typed, and falls back to the configured converter for
            // serialized payloads (e.g. byte[]).
            E payload = event.payloadAs(type, stream.converter());
            total = total.add(mapper.apply(payload));
        }
    }

    @Override
    protected BigDecimal finalValue() {
        return total;
    }

    @Override
    public BigDecimal zero() {
        return BigDecimal.ZERO;
    }

    @Override
    public BigDecimal add(BigDecimal a, BigDecimal b) {
        return a.add(b);
    }

    @Override
    public BigDecimal subtract(BigDecimal a, BigDecimal b) {
        return a.subtract(b);
    }
}
