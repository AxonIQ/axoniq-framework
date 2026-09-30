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

package io.axoniq.framework.springcloud.util;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link DiscoveryClient} for tests, reporting whichever instances a test put into it.
 *
 * @author Allard Buijze
 */
public class RecordingDiscoveryClient implements DiscoveryClient {

    private final Map<String, List<ServiceInstance>> instancesByService = new LinkedHashMap<>();

    public RecordingDiscoveryClient register(String serviceId, ServiceInstance... instances) {
        instancesByService.computeIfAbsent(serviceId, id -> new ArrayList<>()).addAll(List.of(instances));
        return this;
    }

    public RecordingDiscoveryClient deregisterAll() {
        instancesByService.clear();
        return this;
    }

    public RecordingDiscoveryClient deregister(String serviceId, ServiceInstance instance) {
        instancesByService.getOrDefault(serviceId, List.of()).remove(instance);
        return this;
    }

    @Override
    public String description() {
        return "Recording DiscoveryClient";
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        return List.copyOf(instancesByService.getOrDefault(serviceId, List.of()));
    }

    @Override
    public List<String> getServices() {
        return List.copyOf(instancesByService.keySet());
    }
}
