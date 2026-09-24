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

import org.axonframework.common.AxonException;
import org.springframework.cloud.client.ServiceInstance;

import java.io.Serial;

/**
 * Indicates a {@link ServiceInstance} answered a request for its
 * {@link io.axoniq.framework.springcloud.routing.MemberCapabilities capabilities} with a client error.
 * <p>
 * A client error means the instance is reachable but is not serving the capabilities endpoint — it is another service
 * altogether, or an older version of this one. Retrying it on every discovery heartbeat is wasted work, so
 * {@link IgnoreListingDiscoveryMode} treats this exception as the signal to stop asking for a while. A connection
 * failure or a server error is a different matter, and is <em>not</em> reported through this exception: the instance
 * is expected back.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class ServiceInstanceClientException extends AxonException {

    @Serial
    private static final long serialVersionUID = 4408371030502919049L;

    /**
     * Initializes a {@code ServiceInstanceClientException} using the given {@code message}.
     *
     * @param message the message describing the exception
     */
    public ServiceInstanceClientException(String message) {
        super(message);
    }

    /**
     * Initializes a {@code ServiceInstanceClientException} using the given {@code message} and {@code cause}.
     *
     * @param message the message describing the exception
     * @param cause   the client error that led to this exception
     */
    public ServiceInstanceClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
