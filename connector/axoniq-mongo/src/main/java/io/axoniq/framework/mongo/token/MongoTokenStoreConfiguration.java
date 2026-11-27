/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.token;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.temporal.TemporalAmount;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Configuration of a {@link MongoTokenStore}.
 * <p>
 * The configuration is immutable; every {@code with}-style method returns a new instance. Start from {@link #DEFAULT}
 * and adjust the properties that need to differ:
 * <pre>{@code
 * MongoTokenStoreConfiguration configuration =
 *         MongoTokenStoreConfiguration.DEFAULT
 *                 .claimTimeout(Duration.ofSeconds(30))
 *                 .nodeId("node-1");
 * }</pre>
 *
 * @param claimTimeout  the amount of time after which a claim that has not been extended may be taken over by another
 *                      node
 * @param nodeId        the identifier of this node, used to express ownership of tokens
 * @param ensureIndexes whether the {@link MongoTokenStore} creates the unique index on processor name and segment upon
 *                      construction
 * @author Steven van Beelen
 * @since 5.5.0
 */
public record MongoTokenStoreConfiguration(
        TemporalAmount claimTimeout,
        String nodeId,
        boolean ensureIndexes
) {

    /**
     * The default configuration: a claim timeout of 10 seconds, the JVM runtime name as node identifier, and index
     * creation enabled.
     */
    public static final MongoTokenStoreConfiguration DEFAULT = new MongoTokenStoreConfiguration(
            Duration.ofSeconds(10),
            ManagementFactory.getRuntimeMXBean().getName(),
            true
    );

    /**
     * Compact constructor validating the given properties.
     *
     * @param claimTimeout  the claim timeout, may not be {@code null}
     * @param nodeId        the node identifier, may not be {@code null} or empty
     * @param ensureIndexes whether to create the required index upon construction of the token store
     */
    public MongoTokenStoreConfiguration {
        assertNonNull(claimTimeout, "The claim timeout may not be null");
        assertNonEmpty(nodeId, "The nodeId may not be null or empty");
    }

    /**
     * Returns a copy of this configuration with the given {@code claimTimeout}. A claim that has not been extended
     * within this period may be taken over by another node. Defaults to 10 seconds.
     *
     * @param claimTimeout the period after which a claim expires
     * @return a copy of this configuration with the given {@code claimTimeout}
     */
    public MongoTokenStoreConfiguration claimTimeout(TemporalAmount claimTimeout) {
        return new MongoTokenStoreConfiguration(claimTimeout, nodeId, ensureIndexes);
    }

    /**
     * Returns a copy of this configuration with the given {@code nodeId}. Defaults to the JVM runtime name.
     *
     * @param nodeId the identifier of this node, used to express ownership of tokens
     * @return a copy of this configuration with the given {@code nodeId}
     */
    public MongoTokenStoreConfiguration nodeId(String nodeId) {
        return new MongoTokenStoreConfiguration(claimTimeout, nodeId, ensureIndexes);
    }

    /**
     * Returns a copy of this configuration with the given {@code ensureIndexes} flag. When {@code false}, the developer
     * is responsible for creating the unique index on {@code processorName} and {@code segment} beforehand. Defaults to
     * {@code true}.
     *
     * @param ensureIndexes whether the token store creates the required index upon construction
     * @return a copy of this configuration with the given {@code ensureIndexes} flag
     */
    public MongoTokenStoreConfiguration ensureIndexes(boolean ensureIndexes) {
        return new MongoTokenStoreConfiguration(claimTimeout, nodeId, ensureIndexes);
    }
}
