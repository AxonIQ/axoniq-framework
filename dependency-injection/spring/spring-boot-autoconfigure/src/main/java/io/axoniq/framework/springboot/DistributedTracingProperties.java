/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
