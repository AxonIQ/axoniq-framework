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

import org.springframework.cloud.client.ServiceInstance;

import java.util.Objects;

/**
 * An identity for a {@link ServiceInstance} that can safely be used as a map key.
 * <p>
 * {@code ServiceInstance} is an interface, and several Spring Cloud Discovery implementations return instances that do
 * not override {@link Object#equals(Object)}. Two lookups of the same running service therefore routinely produce
 * objects that are unequal, which quietly breaks any bookkeeping keyed on the instance itself. This record pins the
 * identity down to the three fields every implementation populates: the service id, host and port.
 *
 * @param serviceId the {@link ServiceInstance#getServiceId() service id} of the instance
 * @param host      the {@link ServiceInstance#getHost() host} of the instance
 * @param port      the {@link ServiceInstance#getPort() port} of the instance
 * @author Allard Buijze
 * @since 5.4.0
 */
public record ServiceInstanceKey(String serviceId, String host, int port) {

    /**
     * Derives the {@code ServiceInstanceKey} of the given {@code instance}.
     *
     * @param instance the instance to derive a key for
     * @return the key identifying the given {@code instance}
     */
    public static ServiceInstanceKey of(ServiceInstance instance) {
        Objects.requireNonNull(instance, "The instance must not be null.");
        return new ServiceInstanceKey(String.valueOf(instance.getServiceId()),
                                      String.valueOf(instance.getHost()),
                                      instance.getPort());
    }

    @Override
    public String toString() {
        return serviceId + "@" + host + ":" + port;
    }
}
