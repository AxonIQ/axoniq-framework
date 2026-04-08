/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.common.configuration;

/**
 * The configuration of any Axon Framework application.
 * <p>
 * Provides a means to {@link #start()} and {@link #shutdown() stop} the application, besides containing all
 * {@link Component components}.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 5.0.0
 */
public interface AxonConfiguration extends Configuration {

    /**
     * All components defined in this {@code AxonConfiguration} will be started.
     */
    void start();

    /**
     * Shuts down the components defined in this {@code AxonConfiguration}.
     */
    void shutdown();
}
