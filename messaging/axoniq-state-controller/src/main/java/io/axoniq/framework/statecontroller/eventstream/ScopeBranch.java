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
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Objects;
import java.util.Set;

/**
 * One branch of a scope: a tag set, optionally restricted to an explicit set of event payload types.
 * <p>
 * A simple scope is a single branch; a <em>union scope</em> — built on the
 * {@link io.axoniq.framework.statecontroller.History History} surface with
 * {@code of(...).and(types...).or(tagKey, value)...} — is a list of branches that one
 * {@link SourcedEventStream} reads together. At seal time each branch contributes one term to the OR-combined
 * {@link org.axonframework.messaging.eventstreaming.EventCriteria EventCriteria}:
 * <ul>
 *     <li>a {@linkplain #restricted() restricted} branch reads exactly its declared {@link #payloadTypes()} for
 *         its {@link #tags()} — the per-branch type restriction that makes a union precise;</li>
 *     <li>an unrestricted branch reads the union of the event types registered by the conditions declared on the
 *         stream, matching the single-scope behavior.</li>
 * </ul>
 * Instances are immutable values; {@link Set#copyOf(java.util.Collection) copied} on construction so record
 * equality makes branch lists usable as loading-session cache keys.
 * <p>
 * Marked {@link Internal @Internal} because this is plumbing between the runtime's {@code History}
 * implementations and the sourcing engine; user code shapes branches through the {@code History} builder only.
 *
 * @param tags         the tag set identifying the branch's slice; never empty
 * @param payloadTypes the event payload types the branch is restricted to; empty means unrestricted
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
public record ScopeBranch(Set<Tag> tags, Set<Class<?>> payloadTypes) {

    /**
     * Compact constructor validating and defensively copying both sets.
     */
    public ScopeBranch {
        Objects.requireNonNull(tags, "tags must not be null");
        Objects.requireNonNull(payloadTypes, "payloadTypes must not be null");
        if (tags.isEmpty()) {
            throw new IllegalArgumentException("a scope branch requires at least one tag");
        }
        tags = Set.copyOf(tags);
        payloadTypes = Set.copyOf(payloadTypes);
    }

    /**
     * Creates a branch over the given tags with no type restriction — the shape of every simple (non-union)
     * scope.
     *
     * @param tags the tag set identifying the branch's slice
     * @return an unrestricted branch over the given tags
     */
    public static ScopeBranch unrestricted(Set<Tag> tags) {
        return new ScopeBranch(tags, Set.of());
    }

    /**
     * Returns whether this branch carries an explicit type restriction.
     *
     * @return {@code true} when {@link #payloadTypes()} is non-empty
     */
    public boolean restricted() {
        return !payloadTypes.isEmpty();
    }
}
