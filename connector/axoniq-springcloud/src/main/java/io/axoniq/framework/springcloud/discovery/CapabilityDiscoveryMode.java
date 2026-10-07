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
import org.springframework.cloud.client.ServiceInstance;

import java.util.Optional;
import java.util.Set;

/**
 * Discovers the {@link MemberCapabilities} of the {@link ServiceInstance}s reported by Spring Cloud Discovery, and
 * publishes this application's own.
 * <p>
 * Every application is identified by a {@link #localNodeId() node id} of its own, which it reports along with its
 * capabilities. Members are told apart by that id rather than by what discovery reports about them, so that one
 * application reported more than once is still one member, and so that this application recognizes its own instance
 * among the others without having to match its address.
 * <p>
 * Capabilities cannot travel through {@link ServiceInstance#getMetadata() service instance metadata}: that metadata is
 * fixed at registration time, and mutating it after the fact is silently ignored by several discovery
 * implementations — Kubernetes among them. Since the set of subscribed command names changes as an application starts
 * up and is reconfigured, capabilities are instead served over an endpoint of their own, which is what
 * {@link RestCapabilityDiscoveryMode} does.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 5.4.0
 */
public interface CapabilityDiscoveryMode {

    /**
     * Returns the identifier of this application, which it reports to other members along with its capabilities.
     * <p>
     * Unique to the running process: an application that restarts is a new member, which matters because it has not
     * seen what the previous process did.
     *
     * @return the identifier of this application
     */
    String localNodeId();

    /**
     * Publishes the given {@code capabilities} as this application's own, replacing whatever was published before.
     * <p>
     * Called whenever a command or query handler is subscribed to, or unsubscribed from, this application's connector.
     *
     * @param capabilities the messages this application handles, and the command load it asks for
     */
    void updateLocalCapabilities(MemberCapabilities capabilities);

    /**
     * Discovers which member the given {@code serviceInstance} is, and what it handles.
     * <p>
     * An empty {@link Optional} means the instance should be left out of the routing ring entirely — it is not a
     * member of this cluster, or is not currently answering for one. That differs from an advertisement carrying
     * {@link MemberCapabilities#INCAPABLE}, which keeps the instance in the ring as a member that handles nothing.
     * <p>
     * This application's own instance is discovered like any other, answering with this application's
     * {@link #localNodeId() node id}.
     * <p>
     * Called concurrently, once per discovered instance, for every instance of one discovery round. Implementations
     * must therefore be thread-safe.
     *
     * @param serviceInstance the instance to discover
     * @return the member the given {@code serviceInstance} is and what it handles, or {@link Optional#empty()} when it
     * should not be part of the routing ring
     * @throws ServiceInstanceClientException when the given {@code serviceInstance} answers with a client error,
     *                                        indicating it does not serve capabilities at all
     */
    Optional<MemberAdvertisement> discover(ServiceInstance serviceInstance);

    /**
     * Returns this application's own capabilities, as last published through
     * {@link #updateLocalCapabilities(MemberCapabilities)}.
     * <p>
     * A mode is told what this application handles so it can publish it; that same knowledge is what
     * {@link MemberCapabilitiesController} serves to other members, which is why reading it back is part of this
     * contract rather than of any one implementation.
     *
     * @return this application's own capabilities, or {@link MemberCapabilities#INCAPABLE} while nothing has been
     * published yet
     */
    MemberCapabilities localCapabilities();

    /**
     * Signals that the given {@code knownInstances} are the only instances discovery currently reports, allowing this
     * mode to discard whatever it remembers about instances that have gone away.
     * <p>
     * Called at the end of each discovery round. Implementations that keep no per-instance state need not do anything,
     * which is what this default implementation does.
     *
     * @param knownInstances the keys of every instance the current discovery round reported
     */
    default void retainOnly(Set<ServiceInstanceKey> knownInstances) {
        // No per-instance state to discard.
    }
}
