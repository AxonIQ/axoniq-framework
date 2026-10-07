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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link CapabilityDiscoveryMode} for tests, answering with whatever a test configured per instance and recording
 * what it was asked.
 * <p>
 * Each instance answers as a node of its own, identified by {@link #nodeIdOf(ServiceInstance)}, unless a test makes it
 * answer as another node, or as this application through {@link #answeringAsLocal(ServiceInstance)}.
 *
 * @author Allard Buijze
 */
public class RecordingCapabilityDiscoveryMode implements CapabilityDiscoveryMode {

    /**
     * The node id this application answers with.
     */
    public static final String LOCAL_NODE_ID = "local-node";

    private final Map<ServiceInstanceKey, MemberCapabilities> answers = new LinkedHashMap<>();
    private final Map<ServiceInstanceKey, String> nodeIds = new LinkedHashMap<>();
    private final Set<ServiceInstanceKey> localInstances = new LinkedHashSet<>();
    private final Set<ServiceInstanceKey> clientErrors = new LinkedHashSet<>();
    private final Set<ServiceInstanceKey> unknown = new LinkedHashSet<>();
    // Recorded from the threads a discovery round fans out over, so this has to tolerate concurrent adds.
    private final List<ServiceInstanceKey> asked = new CopyOnWriteArrayList<>();
    private final List<Set<ServiceInstanceKey>> retained = new ArrayList<>();

    private MemberCapabilities localCapabilities = MemberCapabilities.INCAPABLE;

    /**
     * Returns the node id the given {@code instance} answers with, unless a test made it answer as another node.
     */
    public static String nodeIdOf(ServiceInstance instance) {
        return ServiceInstanceKey.of(instance).toString();
    }

    public RecordingCapabilityDiscoveryMode answering(ServiceInstance instance, MemberCapabilities capabilities) {
        return answeringAs(instance, nodeIdOf(instance), capabilities);
    }

    /**
     * Makes the given {@code instance} answer as the node with the given {@code nodeId}, as an application reported
     * more than once would.
     */
    public RecordingCapabilityDiscoveryMode answeringAs(ServiceInstance instance,
                                                       String nodeId,
                                                       MemberCapabilities capabilities) {
        ServiceInstanceKey key = ServiceInstanceKey.of(instance);
        unknown.remove(key);
        answers.put(key, capabilities);
        nodeIds.put(key, nodeId);
        return this;
    }

    /**
     * Makes the given {@code instance} answer as this application, with whatever capabilities it last published.
     */
    public RecordingCapabilityDiscoveryMode answeringAsLocal(ServiceInstance instance) {
        localInstances.add(ServiceInstanceKey.of(instance));
        return this;
    }

    /**
     * Makes the given {@code instance} answer with a client error, as an instance not serving the capabilities
     * endpoint would.
     */
    public RecordingCapabilityDiscoveryMode failingWithClientError(ServiceInstance instance) {
        clientErrors.add(ServiceInstanceKey.of(instance));
        return this;
    }

    /**
     * Stops the given {@code instance} answering with a client error, as an instance that has finished starting up
     * would.
     */
    public RecordingCapabilityDiscoveryMode recovering(ServiceInstance instance,
                                                       MemberCapabilities capabilities) {
        clientErrors.remove(ServiceInstanceKey.of(instance));
        return answering(instance, capabilities);
    }

    /**
     * Makes the given {@code instance} report as not being part of the cluster at all.
     */
    public RecordingCapabilityDiscoveryMode reportingUnknown(ServiceInstance instance) {
        unknown.add(ServiceInstanceKey.of(instance));
        return this;
    }

    public List<ServiceInstanceKey> asked() {
        return List.copyOf(asked);
    }

    public List<Set<ServiceInstanceKey>> retained() {
        return List.copyOf(retained);
    }

    @Override
    public String localNodeId() {
        return LOCAL_NODE_ID;
    }

    @Override
    public void updateLocalCapabilities(MemberCapabilities capabilities) {
        this.localCapabilities = capabilities;
    }

    @Override
    public Optional<MemberAdvertisement> discover(ServiceInstance serviceInstance) {
        ServiceInstanceKey key = ServiceInstanceKey.of(serviceInstance);
        asked.add(key);
        if (clientErrors.contains(key)) {
            throw new ServiceInstanceClientException("Instance [" + key + "] does not serve capabilities.");
        }
        if (unknown.contains(key)) {
            return Optional.empty();
        }
        if (localInstances.contains(key)) {
            return Optional.of(new MemberAdvertisement(LOCAL_NODE_ID, localCapabilities));
        }
        return Optional.ofNullable(answers.get(key))
                       .map(capabilities -> new MemberAdvertisement(nodeIds.get(key), capabilities));
    }

    @Override
    public MemberCapabilities localCapabilities() {
        return localCapabilities;
    }

    @Override
    public void retainOnly(Set<ServiceInstanceKey> knownInstances) {
        retained.add(Set.copyOf(knownInstances));
    }
}
