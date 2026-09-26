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

import org.springframework.cloud.client.serviceregistry.Registration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Registration} for tests, standing in for whatever a discovery implementation would report.
 * <p>
 * Implements {@code Registration} rather than plain {@code ServiceInstance} so one type serves both the local
 * registration and the discovered instances. Also reproduces the behaviour the connector has to cope with in the wild:
 * an instance can be created without a URI, and asking for it then throws rather than returning {@code null}, which is
 * what several discovery implementations do before an application has finished registering.
 *
 * @author Allard Buijze
 */
public class TestServiceInstance implements Registration {

    private final String serviceId;
    private final String host;
    private final int port;
    private final Map<String, String> metadata = new HashMap<>();
    private final boolean uriAvailable;

    public static TestServiceInstance instance(String serviceId, String host, int port) {
        return new TestServiceInstance(serviceId, host, port, true);
    }

    public static TestServiceInstance withoutUri(String serviceId) {
        return new TestServiceInstance(serviceId, "unknown", 0, false);
    }

    private TestServiceInstance(String serviceId, String host, int port, boolean uriAvailable) {
        this.serviceId = serviceId;
        this.host = host;
        this.port = port;
        this.uriAvailable = uriAvailable;
    }

    public TestServiceInstance withMetadata(String key, String value) {
        metadata.put(key, value);
        return this;
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public URI getUri() {
        if (!uriAvailable) {
            throw new IllegalStateException("This instance has not registered yet, so it has no URI.");
        }
        return URI.create("http://" + host + ":" + port);
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return serviceId + "@" + host + ":" + port;
    }
}
