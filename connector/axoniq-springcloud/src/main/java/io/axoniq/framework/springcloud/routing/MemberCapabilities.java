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

import org.axonframework.messaging.core.QualifiedName;

import java.util.Objects;
import java.util.Set;

/**
 * The messages a {@link Member} is able to handle, and the relative share of command load it asks for.
 * <p>
 * Capabilities are published by each member and collected by every other member when it joins and on every refresh
 * after, so that the {@link ConsistentHash} ring can route a command only to members that actually subscribed to its
 * name.
 * <p>
 * The {@code loadFactor} applies to {@link #commands() commands} only. Query subscriptions carry no load factor, as
 * a query name is served by whichever member advertises it rather than being hashed onto a ring position.
 *
 * @param loadFactor the relative share of command load this member asks for. A member with twice the load factor of
 *                   another claims twice as many positions on the {@link ConsistentHash} ring, and so receives
 *                   roughly twice as many commands. Must not be negative; a load factor of {@code 0} keeps the
 *                   member off the ring entirely.
 * @param commands   the {@link QualifiedName names} of the commands this member subscribed to
 * @param queries    the {@link QualifiedName names} of the queries this member subscribed to
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 5.4.0
 */
public record MemberCapabilities(int loadFactor, Set<QualifiedName> commands, Set<QualifiedName> queries) {

    /**
     * Capabilities of a member that handles nothing at all.
     * <p>
     * Used both as the starting point for this application's own capabilities before any handler subscribed, and as
     * the stand-in for a member whose capabilities could not be retrieved. In either case the member stays in the
     * ring but is never selected, so a member that is briefly unreachable does not shift the routing of commands it
     * would never have handled.
     */
    public static final MemberCapabilities INCAPABLE = new MemberCapabilities(0, Set.of(), Set.of());

    /**
     * Compact constructor validating that the {@code loadFactor} is not negative and that neither name set is
     * {@code null}, and defensively copying both sets.
     * @param commands the {@link QualifiedName names} of the commands this member subscribed to
     * @param queries the {@link QualifiedName names} of the queries this member subscribed to
     * @param loadFactor the relative share of command load this member asks for
     */
    public MemberCapabilities {
        if (loadFactor < 0) {
            throw new IllegalArgumentException("The load factor cannot be negative, but was [" + loadFactor + "].");
        }
        commands = Set.copyOf(Objects.requireNonNull(commands, "The commands must not be null."));
        queries = Set.copyOf(Objects.requireNonNull(queries, "The queries must not be null."));
    }

    /**
     * Indicates whether the member holding these capabilities subscribed to a command with the given
     * {@code commandName}.
     *
     * @param commandName the {@link QualifiedName} of the command to check for
     * @return {@code true} when this member handles commands of the given {@code commandName}, {@code false}
     * otherwise
     */
    public boolean handlesCommand(QualifiedName commandName) {
        return commands.contains(commandName);
    }

    /**
     * Indicates whether the member handles queries by the given {@code queryName}.
     *
     * @param queryName the name of the query to check for
     * @return {@code true} when the member handles queries by the given {@code queryName}, {@code false} otherwise
     */
    public boolean handlesQuery(QualifiedName queryName) {
        return queries.contains(queryName);
    }
}
