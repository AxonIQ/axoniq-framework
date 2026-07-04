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
import io.axoniq.framework.statecontroller.eventstream.EventStream;
import io.axoniq.framework.statecontroller.eventstream.ScopeBranch;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A {@link History} narrowed to one or more {@link ScopeBranch branches}, translating each read into a lazy
 * condition on the session's current {@link EventStream} for that scope.
 * <p>
 * Each read method looks the scope's stream up at declaration time via {@link HistorySession#scopeFor(List)}, so
 * a condition declared after the decision's first resolution lands on a fresh stream and is answered by a
 * supplementary read (see {@link HistorySession}).
 * <p>
 * The builder methods refine the branch list immutably: {@link #of(String, Object) of(...)} narrows the current
 * (last) branch to a composite tag set, {@link #and(Class[]) and(...)} restricts the current branch's event
 * types, and {@link #or(String, Object) or(...)} opens a new branch — the union-scope case where one history
 * spans differently-tagged slices read as a single consistency boundary.
 * <p>
 * Marked {@link Internal @Internal} because instances are created by narrowing the injected root {@link History};
 * user code only sees the interface.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
final class ScopedHistory implements History {

    private final HistorySession session;
    private final List<ScopeBranch> branches;

    ScopedHistory(HistorySession session, Set<Tag> tags) {
        this(session, List.of(ScopeBranch.unrestricted(tags)));
    }

    private ScopedHistory(HistorySession session, List<ScopeBranch> branches) {
        this.session = Objects.requireNonNull(session, "session must not be null");
        this.branches = List.copyOf(branches);
    }

    @Override
    public History of(String tagKey, Object tagValue) {
        Objects.requireNonNull(tagKey, "tagKey must not be null");
        Objects.requireNonNull(tagValue, "tagValue must not be null");
        return of(Map.of(tagKey, tagValue));
    }

    @Override
    public History of(Map<String, ?> moreTags) {
        Objects.requireNonNull(moreTags, "tags must not be null");
        ScopeBranch current = branches.getLast();
        Set<Tag> narrowed = new HashSet<>(current.tags());
        for (Map.Entry<String, ?> e : moreTags.entrySet()) {
            narrowed.add(new Tag(
                    Objects.requireNonNull(e.getKey(), "tag key must not be null"),
                    Objects.requireNonNull(e.getValue(), "tag value must not be null").toString()));
        }
        return withCurrentBranch(new ScopeBranch(narrowed, current.payloadTypes()));
    }

    @Override
    public History and(Class<?>... types) {
        Objects.requireNonNull(types, "types must not be null");
        if (types.length == 0) {
            throw new IllegalArgumentException("and(...) requires at least one event type");
        }
        ScopeBranch current = branches.getLast();
        Set<Class<?>> restricted = new HashSet<>(current.payloadTypes());
        for (Class<?> type : types) {
            restricted.add(Objects.requireNonNull(type, "type must not be null"));
        }
        return withCurrentBranch(new ScopeBranch(current.tags(), restricted));
    }

    @Override
    public History or(String tagKey, Object tagValue) {
        Objects.requireNonNull(tagKey, "tagKey must not be null");
        Objects.requireNonNull(tagValue, "tagValue must not be null");
        List<ScopeBranch> extended = new ArrayList<>(branches);
        extended.add(ScopeBranch.unrestricted(Set.of(new Tag(tagKey, tagValue.toString()))));
        return new ScopedHistory(session, extended);
    }

    /**
     * Returns a copy of this history with the current (last) branch replaced by the given refinement.
     */
    private History withCurrentBranch(ScopeBranch refined) {
        List<ScopeBranch> updated = new ArrayList<>(branches);
        updated.set(updated.size() - 1, refined);
        return new ScopedHistory(session, updated);
    }

    @Override
    public BooleanCondition has(Class<?> type) {
        return stream().contains(type);
    }

    @Override
    public BooleanCondition never(Class<?> type) {
        return stream().contains(type).not();
    }

    @Override
    public NumericCondition<Long> count(Class<?>... types) {
        return stream().count(types);
    }

    @Override
    public <E> NumericCondition<BigDecimal> total(Class<E> type, Function<? super E, BigDecimal> amount) {
        return stream().sum(type, amount);
    }

    @Override
    public <E> NumericCondition<Long> totalLong(Class<E> type, ToLongFunction<? super E> amount) {
        return stream().sumLong(type, amount);
    }

    @Override
    public <E> OptionalCondition<E> latest(Class<E> type) {
        return stream().latest(type);
    }

    @Override
    public Condition<@Nullable Object> latestOf(Class<?>... types) {
        return stream().latestOf(types).map(selected -> selected.orElse(null));
    }

    @Override
    public <E> OptionalCondition<E> first(Class<E> type) {
        return stream().first(type);
    }

    /**
     * Returns the scope's current stream, looked up per read so late declarations land on a fresh (unsealed)
     * stream rather than a stale reference.
     */
    private EventStream stream() {
        return session.scopeFor(branches);
    }
}
