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

package io.axoniq.framework.springcloud.routing;

import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.util.Objects;

/**
 * A single node in the cluster of applications distributing messages through Spring Cloud Discovery.
 * <p>
 * A {@code Member} is derived from a {@code ServiceInstance} reported by the discovery client. It is what the
 * {@link ConsistentHash} ring resolves a command's routing key to, and what a query name resolves to when the members
 * advertising it are rotated over. The {@link #local() local} flag decides how a message addressed to this member is
 * delivered: a local member is handled through the registered handler of this application's own connector, while a
 * remote member is reached over HTTP at its {@link #endpoint() endpoint}.
 * <p>
 * The {@code endpoint} of a local member may be {@code null}. Several Spring Cloud Discovery implementations do not
 * expose a URI until the application has completed its registration, and throw when one is requested before that
 * point. A local member is therefore allowed to exist without an endpoint, since it is never reached over HTTP
 * anyway. A remote member without an endpoint cannot be reached at all, and is rejected on construction.
 *
 * @param name     the unique name of this member within the cluster, derived from its service id and URI
 * @param endpoint the base {@link URI} to reach this member over HTTP, or {@code null} for a
 *                 {@link #local() local} member whose URI is not yet known.
 * @param local    {@code true} when this member represents the application it is constructed in, {@code false}
 *                 when it represents another node.
 * @author Allard Buijze
 * @since 5.4.0
 */
public record Member(String name, @Nullable URI endpoint, boolean local) {

    /**
     * Compact constructor validating that the {@code name} is present, and that a non-{@link #local() local} member
     * carries an {@code endpoint} to reach it at.
     */
    @SuppressWarnings("MissingJavadoc")
    public Member {
        Objects.requireNonNull(name, "The member name must not be null.");
        if (!local && endpoint == null) {
            throw new IllegalArgumentException(
                    "A remote member requires an endpoint to reach it at, but none was given for [" + name + "]."
            );
        }
    }

    /**
     * Constructs a local {@code Member} with the given {@code name} and no endpoint.
     * <p>
     * Intended for the window between application start-up and completed discovery registration, during which this
     * application's own URI is not yet available.
     *
     * @param name the unique name of this member within the cluster
     * @return a local {@code Member} without an endpoint
     */
    public static Member unregisteredLocalMember(String name) {
        return new Member(name, null, true);
    }

    @Override
    public String toString() {
        return name + (local ? " [local]" : " [" + endpoint + "]");
    }
}
