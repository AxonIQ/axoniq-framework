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

package io.axoniq.framework.springcloud.discovery;

import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import org.axonframework.common.digest.Digester;
import org.axonframework.messaging.core.QualifiedName;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The wire representation of a member's {@link MemberCapabilities}, as served by
 * {@link MemberCapabilitiesController} and read by {@link RestCapabilityDiscoveryMode}.
 * <p>
 * This record exists separately from {@code MemberCapabilities} so that the JSON on the wire is fixed by a type of
 * this module's own, rather than following the record shape of
 * {@link QualifiedName}. Names travel as plain strings, which is both the obvious JSON and stable against changes to
 * {@code QualifiedName} itself. Fields are ordered lists rather than sets purely so that the serialized bytes of
 * unchanged capabilities are themselves unchanged, which is what makes the {@code ETag} on the endpoint meaningful.
 * <p>
 * The {@code queries} field may be absent from a member running a version that predates query distribution, so
 * members reading it must tolerate an absent or empty list.
 *
 * @param loadFactor the relative share of command load the member asks for
 * @param commands   the {@link QualifiedName#name() names} of the commands the member subscribed to, sorted
 * @param queries    the {@link QualifiedName#name() names} of the queries the member subscribed to, sorted
 * @author Allard Buijze
 * @since 5.4.0
 */
public record MemberCapabilitiesPayload(int loadFactor, List<String> commands, List<String> queries) {

    /**
     * Compact constructor defaulting {@code null} name lists to empty, so a payload from a member that omits a field
     * altogether still reads.
     */
    @SuppressWarnings("MissingJavadoc")
    public MemberCapabilitiesPayload {
        commands = commands == null ? List.of() : List.copyOf(commands);
        queries = queries == null ? List.of() : List.copyOf(queries);
    }

    /**
     * Converts the given {@code capabilities} into their wire representation.
     *
     * @param capabilities the capabilities to represent on the wire
     * @return the wire representation of the given {@code capabilities}
     */
    public static MemberCapabilitiesPayload from(MemberCapabilities capabilities) {
        Objects.requireNonNull(capabilities, "The capabilities must not be null.");
        return new MemberCapabilitiesPayload(capabilities.loadFactor(),
                                             sortedNames(capabilities.commands()),
                                             sortedNames(capabilities.queries()));
    }

    private static List<String> sortedNames(Set<QualifiedName> names) {
        return names.stream().map(QualifiedName::name).sorted().toList();
    }

    /**
     * Converts this wire representation back into {@link MemberCapabilities}.
     *
     * @return the capabilities this payload represents
     */
    public MemberCapabilities toCapabilities() {
        return new MemberCapabilities(loadFactor, qualifiedNames(commands), qualifiedNames(queries));
    }

    private static Set<QualifiedName> qualifiedNames(List<String> names) {
        return names.stream().map(QualifiedName::new).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns an entity tag identifying the content of this payload, for use as the HTTP {@code ETag} of the
     * capabilities endpoint and the {@code If-None-Match} sent by a member polling it.
     * <p>
     * The tag is derived from the payload's own fields rather than from its serialized bytes, so it does not depend on
     * how the surrounding web stack chooses to render the JSON. Because the name lists are sorted, two members
     * reporting the same capabilities produce the same tag.
     *
     * @return an entity tag for this payload's content, without the surrounding quotes an {@code ETag} header needs
     */
    public String entityTag() {
        return Digester.md5Hex(loadFactor + "|" + String.join(",", commands) + "|" + String.join(",", queries));
    }

    /**
     * Returns this payload's {@link #entityTag()} wrapped in the quotes an {@code ETag} or {@code If-None-Match}
     * header value requires.
     *
     * @return this payload's entity tag, ready to be used as a header value
     */
    public String quotedEntityTag() {
        return "\"" + entityTag() + "\"";
    }
}
