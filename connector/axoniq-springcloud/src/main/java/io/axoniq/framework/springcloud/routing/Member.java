/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.routing;

import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.util.Objects;

/**
 * A single node in the cluster of applications distributing messages through Spring Cloud Discovery.
 * <p>
 * A {@code Member} is identified by the node id the application reports about itself, rather than by anything the
 * discovery client reports about it, so that an application reported more than once is still one member. Its endpoint
 * comes from the {@code ServiceInstance} the discovery client reported it as. A {@code Member} is what the
 * {@link ConsistentHash} ring resolves a command's routing key to, and what a query name resolves to when the members
 * advertising it are rotated over. The {@link #local() local} flag decides how a message addressed to this member is
 * delivered: a local member is handled through the registered handler of this application's own connector, while a
 * remote member is reached over HTTP at its {@link #endpoint() endpoint}.
 * <p>
 * A local member has no {@code endpoint}, since it is never reached over HTTP. A remote member without an endpoint
 * cannot be reached at all, and is rejected on construction.
 *
 * @param name     the node id of this member, unique to the application process it represents. It is also what
 *                 decides the member's positions on the {@link ConsistentHash} ring, so every member of the cluster
 *                 places it the same way
 * @param endpoint the base {@link URI} to reach this member over HTTP, or {@code null} for a {@link #local() local}
 *                 member
 * @param local    {@code true} when this member represents the application it is constructed in, {@code false}
 *                 when it represents another node
 * @author Allard Buijze
 * @since 5.4.0
 */
public record Member(String name, @Nullable URI endpoint, boolean local) {

    /**
     * Compact constructor validating that the {@code name} is present, and that a non-{@link #local() local} member
     * carries an {@code endpoint} to reach it at.
     * @param endpoint the base {@link URI} to reach this member over HTTP, or {@code null} for a
     *                 {@link #local() local} member
     * @param local    {@code true} when this member represents the application it is constructed in, {@code false}
     * @param name     the node id of this member, unique to the application process it represents
     */
    public Member {
        Objects.requireNonNull(name, "The member name must not be null.");
        if (!local && endpoint == null) {
            throw new IllegalArgumentException(
                    "A remote member requires an endpoint to reach it at, but none was given for [" + name + "]."
            );
        }
    }

    /**
     * Constructs the local {@code Member} representing the application it is constructed in.
     *
     * @param name the node id of this application
     * @return a local {@code Member} without an endpoint
     */
    public static Member localMember(String name) {
        return new Member(name, null, true);
    }

    @Override
    public String toString() {
        return name + (local ? " [local]" : " [" + endpoint + "]");
    }
}
