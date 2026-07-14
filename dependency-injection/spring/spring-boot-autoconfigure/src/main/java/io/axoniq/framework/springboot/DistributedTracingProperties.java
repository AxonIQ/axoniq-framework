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

package io.axoniq.framework.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for AxoniqFramework distributed-connector tracing.
 * <p>
 * Binds the connector-specific toggles of the shared {@code axon.tracing.*} property namespace. The generic tracing
 * properties (master switch, per-component toggles, attribute providers) are bound by the open-source
 * {@code TracingProperties} from the Axon Framework Spring Boot autoconfigure module; this class adds only the
 * toggles for the distributed bus connectors shipped with {@code axoniq-distributed-messaging}:
 * <pre>{@code
 * axon:
 *   tracing:
 *     command-bus-connector:
 *       enabled: true
 *     query-bus-connector:
 *       enabled: true
 * }</pre>
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@ConfigurationProperties(prefix = "axon.tracing")
public class DistributedTracingProperties {

    /**
     * Tracing settings for the distributed {@code CommandBusConnector}.
     */
    private final CommandBusConnector commandBusConnector = new CommandBusConnector();

    /**
     * Tracing settings for the distributed {@code QueryBusConnector}.
     */
    private final QueryBusConnector queryBusConnector = new QueryBusConnector();

    /**
     * Returns the tracing settings for the distributed {@code CommandBusConnector}.
     *
     * @return the {@code CommandBusConnector} tracing settings, never {@code null}
     */
    public CommandBusConnector getCommandBusConnector() {
        return commandBusConnector;
    }

    /**
     * Returns the tracing settings for the distributed {@code QueryBusConnector}.
     *
     * @return the {@code QueryBusConnector} tracing settings, never {@code null}
     */
    public QueryBusConnector getQueryBusConnector() {
        return queryBusConnector;
    }

    /**
     * Tracing settings for the distributed {@code CommandBusConnector}.
     */
    public static class CommandBusConnector {

        /**
         * Whether the distributed {@code CommandBusConnector} is decorated with tracing. Defaults to {@code true}.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * Tracing settings for the distributed {@code QueryBusConnector}.
     */
    public static class QueryBusConnector {

        /**
         * Whether the distributed {@code QueryBusConnector} is decorated with tracing. Defaults to {@code true}.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
