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

/**
 * A {@link CapabilityDiscoveryMode} for tests, answering with whatever a test configured per instance and recording
 * what it was asked.
 *
 * @author Allard Buijze
 */
public class RecordingCapabilityDiscoveryMode implements CapabilityDiscoveryMode {

    private final Map<ServiceInstanceKey, MemberCapabilities> answers = new LinkedHashMap<>();
    private final Set<ServiceInstanceKey> clientErrors = new LinkedHashSet<>();
    private final Set<ServiceInstanceKey> unknown = new LinkedHashSet<>();
    private final List<ServiceInstanceKey> asked = new ArrayList<>();
    private final List<Set<ServiceInstanceKey>> retained = new ArrayList<>();

    private MemberCapabilities localCapabilities = MemberCapabilities.INCAPABLE;
    private ServiceInstance localInstance;

    public RecordingCapabilityDiscoveryMode answering(ServiceInstance instance, MemberCapabilities capabilities) {
        answers.put(ServiceInstanceKey.of(instance), capabilities);
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
        ServiceInstanceKey key = ServiceInstanceKey.of(instance);
        clientErrors.remove(key);
        answers.put(key, capabilities);
        return this;
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

    public ServiceInstance localInstance() {
        return localInstance;
    }

    @Override
    public void updateLocalCapabilities(ServiceInstance localInstance, MemberCapabilities capabilities) {
        this.localInstance = localInstance;
        this.localCapabilities = capabilities;
        answers.put(ServiceInstanceKey.of(localInstance), capabilities);
    }

    @Override
    public Optional<MemberCapabilities> capabilities(ServiceInstance serviceInstance) {
        ServiceInstanceKey key = ServiceInstanceKey.of(serviceInstance);
        asked.add(key);
        if (clientErrors.contains(key)) {
            throw new ServiceInstanceClientException("Instance [" + key + "] does not serve capabilities.");
        }
        if (unknown.contains(key)) {
            return Optional.empty();
        }
        return Optional.ofNullable(answers.get(key));
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
