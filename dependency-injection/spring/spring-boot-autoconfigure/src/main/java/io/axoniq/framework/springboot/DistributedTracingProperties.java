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
 * Spring Boot configuration properties for the distributed-connector tracing.
 * <p>
 * Binds the connector-specific toggles of the shared {@code axon.tracing.*} property namespace. The generic tracing
 * properties (master switch, per-component toggles, attribute providers) use that same namespace; this class adds the
 * toggles for the distributed bus connectors:
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
     * The tracing settings for the distributed {@code CommandBusConnector}.
     *
     * @return the {@code CommandBusConnector} tracing settings, never {@code null}
     */
    public CommandBusConnector getCommandBusConnector() {
        return commandBusConnector;
    }

    /**
     * The tracing settings for the distributed {@code QueryBusConnector}.
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

        /**
         * Whether tracing is enabled for the distributed {@code CommandBusConnector}.
         *
         * @return {@code true} when tracing is enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Whether to enable tracing for the distributed {@code CommandBusConnector}.
         *
         * @param enabled whether tracing is enabled
         */
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

        /**
         * Whether tracing is enabled for the distributed {@code QueryBusConnector}.
         *
         * @return {@code true} when tracing is enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Whether to enable tracing for the distributed {@code QueryBusConnector}.
         *
         * @param enabled whether tracing is enabled
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
