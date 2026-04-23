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

package io.axoniq.framework.testcontainer;

import org.springframework.boot.testcontainers.service.connection.ContainerConnectionDetailsFactory;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionSource;

/**
 * {@link ContainerConnectionDetailsFactory} that produces {@link AxonServerConnectionDetails} from an
 * {@link AxonServerContainer} annotated with
 * {@link org.springframework.boot.testcontainers.service.connection.ServiceConnection}.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see AxonServerConnectionDetails
 */
class AxonServerContainerConnectionDetailsFactory
        extends ContainerConnectionDetailsFactory<AxonServerContainer, AxonServerConnectionDetails> {

    AxonServerContainerConnectionDetailsFactory() {
        super(ANY_CONNECTION_NAME, "io.axoniq.framework.testcontainer.AxonServerContainer");
    }

    @Override
    protected AxonServerConnectionDetails getContainerConnectionDetails(
            ContainerConnectionSource<AxonServerContainer> source) {
        return new AxonServerContainerConnectionDetails(source);
    }

    private static final class AxonServerContainerConnectionDetails
            extends ContainerConnectionDetails<AxonServerContainer>
            implements AxonServerConnectionDetails {

        AxonServerContainerConnectionDetails(ContainerConnectionSource<AxonServerContainer> source) {
            super(source);
        }

        @Override
        public String routingServers() {
            return getContainer().getAxonServerAddress();
        }
    }
}
