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

package io.axoniq.framework.statecontroller.history;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The unbound {@link History} injected into a {@code @Decide} method. It is a pure factory for narrowed
 * histories: {@link #of(String, Object)}, {@link #of(Class[])} and {@link #matching(EventCriteria)} mint a
 * {@link SourcedHistory} via the owning {@link HistoryFactory}, while every read method fails fast — reading is
 * only meaningful after narrowing, never on the root parameter itself.
 * <p>
 * The fluent builder operations {@link #and(Class[])}, {@link #or(String, Object)} and {@link #or(Class[])} extend
 * a term and so are only valid on an in-progress (already-started) builder; invoking them on the bare root, where
 * no term has been started yet, fails fast like the read methods.
 * <p>
 * Marked {@link Internal @Internal} because instances are minted by {@link HistoryFactory}; user code only ever
 * sees the {@link History} interface.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
public final class RootHistory implements History {

    private static final String UNBOUND_MESSAGE =
            "call of(...) or matching(...) before reading history";

    private final HistoryFactory factory;

    /**
     * Creates a {@code RootHistory} that mints narrowed histories through the given {@code factory}.
     *
     * @param factory the {@link HistoryFactory} used to mint and cache narrowed {@link SourcedHistory} instances
     */
    public RootHistory(HistoryFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory must not be null");
    }

    @Override
    public History of(String tagKey, Object tagValue) {
        Objects.requireNonNull(tagKey, "tagKey must not be null");
        Objects.requireNonNull(tagValue, "tagValue must not be null");
        return factory.ofTag(tagKey, tagValue);
    }

    @Override
    public History of(Class<?>... types) {
        return factory.ofTypes(types);
    }

    @Override
    public History matching(EventCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria must not be null");
        return factory.narrow(criteria);
    }

    @Override
    public History and(Class<?>... types) {
        throw unbound();
    }

    @Override
    public History or(String tagKey, Object tagValue) {
        throw unbound();
    }

    @Override
    public History or(Class<?>... types) {
        throw unbound();
    }

    @Override
    public boolean has(Class<?> type) {
        throw unbound();
    }

    @Override
    public boolean never(Class<?> type) {
        throw unbound();
    }

    @Override
    public boolean lastWas(Class<?> type) {
        throw unbound();
    }

    @Override
    public <E> Optional<E> latest(Class<E> type) {
        throw unbound();
    }

    @Override
    public @Nullable Object latestOf(Class<?>... types) {
        throw unbound();
    }

    @Override
    public <E> Optional<E> first(Class<E> type) {
        throw unbound();
    }

    @Override
    public long count(Class<?>... types) {
        throw unbound();
    }

    @Override
    public <E> BigDecimal total(Class<E> type, Function<? super E, BigDecimal> mapper) {
        throw unbound();
    }

    @Override
    public <E> Optional<History.Entry<E>> entry(Class<E> type) {
        throw unbound();
    }

    private static IllegalStateException unbound() {
        return new IllegalStateException(UNBOUND_MESSAGE);
    }
}
