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

package io.axoniq.framework.statecontroller.runtime;

import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * The unbound root {@link History} injected into a state-controlled {@code @CommandHandler} method.
 * <p>
 * Only {@link #of(String, Object)} and {@link #of(Map)} are supported: they narrow to a {@link ScopedHistory}
 * over the session. Every read method fails fast with {@link IllegalStateException} — a decision must always name
 * the scope it reads, never query the store globally by accident.
 * <p>
 * Marked {@link Internal @Internal} because instances are created by {@link HistorySession}; user code only sees
 * the {@link History} interface.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
final class RootHistory implements History {

    private final HistorySession session;

    RootHistory(HistorySession session) {
        this.session = Objects.requireNonNull(session, "session must not be null");
    }

    @Override
    public History of(String tagKey, Object tagValue) {
        Objects.requireNonNull(tagKey, "tagKey must not be null");
        Objects.requireNonNull(tagValue, "tagValue must not be null");
        return new ScopedHistory(session, Set.of(new Tag(tagKey, tagValue.toString())));
    }

    @Override
    public History of(Map<String, ?> tags) {
        Objects.requireNonNull(tags, "tags must not be null");
        if (tags.isEmpty()) {
            throw new IllegalArgumentException("of(...) requires at least one tag");
        }
        Set<Tag> tagSet = new HashSet<>(tags.size());
        for (Map.Entry<String, ?> e : tags.entrySet()) {
            tagSet.add(new Tag(
                    Objects.requireNonNull(e.getKey(), "tag key must not be null"),
                    Objects.requireNonNull(e.getValue(), "tag value must not be null").toString()));
        }
        return new ScopedHistory(session, Set.copyOf(tagSet));
    }

    @Override
    public BooleanCondition has(Class<?> type) {
        throw unbound();
    }

    @Override
    public BooleanCondition never(Class<?> type) {
        throw unbound();
    }

    @Override
    public NumericCondition<Long> count(Class<?>... types) {
        throw unbound();
    }

    @Override
    public <E> NumericCondition<BigDecimal> total(Class<E> type, Function<? super E, BigDecimal> amount) {
        throw unbound();
    }

    @Override
    public <E> NumericCondition<Long> totalLong(Class<E> type, ToLongFunction<? super E> amount) {
        throw unbound();
    }

    @Override
    public <E> OptionalCondition<E> latest(Class<E> type) {
        throw unbound();
    }

    @Override
    public Condition<@Nullable Object> latestOf(Class<?>... types) {
        throw unbound();
    }

    @Override
    public <E> OptionalCondition<E> first(Class<E> type) {
        throw unbound();
    }

    private static IllegalStateException unbound() {
        return new IllegalStateException(
                "This History is unbound. Narrow it to a scope first, for example: "
                        + "history.of(\"account\", cmd.accountId()).has(AccountClosed.class)");
    }
}
