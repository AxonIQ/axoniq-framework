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

package io.axoniq.framework.messaging.eventstreaming.safepoint;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link ComponentDescriptor} that walks a {@link DescribableComponent}'s describe graph and captures
 * <strong>every</strong> instance assignable to a caller-supplied type.
 * <p>
 * <strong>Capture semantics: deep, not first-match.</strong> The traversal does not short-circuit on the
 * first hit. It walks the full describe graph reachable through matching properties and emits every matching
 * component to the {@link Stream} returned by {@link #capture(DescribableComponent)}. This is deliberate: it
 * lets the caller take the union over an arbitrary depth of nested matches (for example, two components in
 * the same decorator chain that each declare a capability), and it removes ambiguity about traversal order
 * ("first in DFS? in BFS?"). Callers that only want a single match should post-process the stream (e.g.
 * {@code .findFirst()}); the descriptor itself takes no opinion on cardinality.
 * <p>
 * <strong>Walk scope: narrow.</strong> Only properties whose value is an instance of the captor's configured
 * {@code type} are descended into. Properties of any other type (repositories, factories, stores, identifiers,
 * primitive values, ...) are ignored. This keeps the walk tight even on rich component graphs and avoids
 * pulling in unrelated object hierarchies. Visited components are tracked by identity to prevent infinite
 * recursion when a graph contains cycles.
 * <p>
 * Construct a fresh descriptor per logical capture. Use {@link #capturing(Class)} and drive the descriptor
 * directly as a {@code ComponentDescriptor}, or call the convenience
 * {@link #capture(DescribableComponent)} method:
 * <pre>{@code
 * // Direct usage:
 * var descriptor = CapturingComponentDescriptor.capturing(EventHandlingComponent.class);
 * component.describeTo(descriptor);
 * Stream<EventHandlingComponent> result = descriptor.captured();
 *
 * // Convenience (also captures the root itself if it matches):
 * Stream<EventHandlingComponent> result =
 *     CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(root);
 * }</pre>
 * <p>
 * Not thread-safe: an instance must not be shared across threads.
 *
 * @param <T> the {@link DescribableComponent} type to capture and recurse into
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class CapturingComponentDescriptor<T extends DescribableComponent> implements ComponentDescriptor {

    /**
     * Creates a fresh descriptor that captures every instance of {@code type} encountered while a describe
     * graph is walked through it.
     *
     * @param type the {@link DescribableComponent} type to capture and recurse into
     * @param <T>  the captured type
     * @return a fresh, empty descriptor configured for {@code type}
     */
    public static <T extends DescribableComponent> CapturingComponentDescriptor<T> capturing(Class<T> type) {
        return new CapturingComponentDescriptor<>(type);
    }

    private final Class<T> type;
    private final Set<T> captured = Collections.newSetFromMap(new IdentityHashMap<>());

    private CapturingComponentDescriptor(Class<T> type) {
        this.type = type;
    }

    /**
     * Convenience: walks {@code root}'s describe graph and returns a stream of every captured-type instance
     * found. Unlike a plain {@code describeTo} call, {@code root} itself is also captured when it matches
     * the configured type.
     *
     * @param root the component to start the traversal from
     * @return a stream of every captured-type instance currently held by this descriptor
     */
    public Stream<T> capture(DescribableComponent root) {
        captureIfMatch(root);
        return captured();
    }

    /**
     * Returns the components captured so far by this descriptor, in identity-unique order.
     *
     * @return a stream of every captured-type instance accumulated through prior describe-graph walks
     */
    public Stream<T> captured() {
        return captured.stream();
    }

    private void captureIfMatch(@Nullable Object candidate) {
        if (!type.isInstance(candidate)) {
            return;
        }
        T component = type.cast(candidate);
        if (captured.add(component)) {
            component.describeTo(this);
        }
    }

    @Override
    public void describeProperty(String name, @Nullable Object object) {
        captureIfMatch(object);
    }

    @Override
    public void describeProperty(String name, @Nullable Collection<?> collection) {
        if (collection == null) {
            return;
        }
        for (Object item : collection) {
            captureIfMatch(item);
        }
    }

    @Override
    public void describeProperty(String name, @Nullable Map<?, ?> map) {
        if (map == null) {
            return;
        }
        for (Object value : map.values()) {
            captureIfMatch(value);
        }
    }

    @Override
    public void describeProperty(String name, @Nullable String value) {
        // no-op
    }

    @Override
    public void describeProperty(String name, @Nullable Long value) {
        // no-op
    }

    @Override
    public void describeProperty(String name, @Nullable Boolean value) {
        // no-op
    }

    @Override
    public String describe() {
        return "";
    }
}
