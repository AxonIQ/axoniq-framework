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

package io.axoniq.framework.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The consumer settings that the Kafka event sources apply on top of their configured consumer properties, and the
 * warnings about configured settings they do not use.
 * <p>
 * Both kinds of source disable automatic commits, and read with {@code isolation.level=read_committed} unless their
 * own consumer properties set another level. A {@link #forStreaming(Map) streaming source} removes any
 * {@code group.id} and always sets {@code auto.offset.reset=none}. A {@link #forGroup(Map, String) subscribing source}
 * sets its configured group and {@code auto.offset.reset=none}, unless its properties set an offset reset.
 * <p>
 * Internal, because these settings are implementation details of the sources that their public documentation
 * describes.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public final class ConsumerSettings {

    private static final String READ_COMMITTED = "read_committed";
    private static final String READ_UNCOMMITTED = "read_uncommitted";
    private static final String NONE = "none";

    private ConsumerSettings() {
        // static helpers
    }

    /**
     * Returns the settings of a consumer of a streaming source: the given {@code properties}, without a group
     * identifier, without automatic commits and offset reset, and reading committed records unless configured
     * otherwise.
     *
     * @param properties the consumer properties configured for the source
     * @return a new map with the settings of the source's consumers
     */
    public static Map<String, Object> forStreaming(Map<String, Object> properties) {
        Map<String, Object> settings = new HashMap<>(properties);
        settings.remove(ConsumerConfig.GROUP_ID_CONFIG);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, NONE);
        settings.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED);
        return settings;
    }

    /**
     * Returns the settings of a consumer of a subscribing source: the given {@code properties}, with the given
     * {@code groupId}, without automatic commits, without offset reset and reading committed records unless
     * configured otherwise.
     *
     * @param properties the consumer properties configured for the source
     * @param groupId    the consumer group the source joins
     * @return a new map with the settings of the source's consumers
     */
    public static Map<String, Object> forGroup(Map<String, Object> properties, String groupId) {
        Map<String, Object> settings = new HashMap<>(properties);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        settings.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, NONE);
        settings.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED);
        return settings;
    }

    /**
     * Returns whether a source with the given consumer {@code properties} reads uncommitted records, which it only
     * does when its properties set {@code isolation.level=read_uncommitted}.
     *
     * @param properties the consumer properties configured for the source
     * @return {@code true} if the source reads uncommitted records
     */
    public static boolean readsUncommitted(Map<String, Object> properties) {
        Object isolation = properties.getOrDefault(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED);
        return READ_UNCOMMITTED.equalsIgnoreCase(String.valueOf(isolation));
    }

    /**
     * Logs a warning to the given {@code logger} about the settings among the given {@code properties} that a
     * streaming source does not use: a group identifier, automatic commits and an offset reset.
     *
     * @param properties the consumer properties configured for the source
     * @param logger     the logger of the source
     */
    public static void warnAboutIgnoredStreamingSettings(Map<String, Object> properties, Logger logger) {
        Map<String, String> ignored = new LinkedHashMap<>();
        if (properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            ignored.put(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(properties.get(ConsumerConfig.GROUP_ID_CONFIG)));
        }
        Object autoCommit = properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (enablesAutoCommit(autoCommit)) {
            ignored.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        }
        Object offsetReset = properties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        if (offsetReset != null && !NONE.equalsIgnoreCase(String.valueOf(offsetReset))) {
            ignored.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, String.valueOf(offsetReset));
        }
        if (!ignored.isEmpty()) {
            logger.warn("The consumer settings {} of a streamable Kafka source are not used: the processor keeps the "
                                + "position in its token store, so the source joins no consumer group, commits no "
                                + "offsets and applies its retention gap policy instead of an offset reset.", ignored);
        }
    }

    /**
     * Logs warnings to the given {@code logger} about the settings among the given {@code properties} that a
     * subscribing source for the given {@code groupId} does not use: another group identifier and automatic commits.
     *
     * @param properties the consumer properties configured for the source
     * @param groupId    the consumer group the source joins
     * @param logger     the logger of the source
     */
    public static void warnAboutIgnoredGroupSettings(Map<String, Object> properties, String groupId, Logger logger) {
        Object configuredGroupId = properties.get(ConsumerConfig.GROUP_ID_CONFIG);
        if (configuredGroupId != null && !groupId.equals(String.valueOf(configuredGroupId))) {
            logger.warn("The {} [{}] in the consumer properties of the Kafka source for consumer group [{}] is not "
                                + "used: the source joins the consumer group it is configured with.",
                        ConsumerConfig.GROUP_ID_CONFIG, configuredGroupId, groupId);
        }
        Object autoCommit = properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (enablesAutoCommit(autoCommit)) {
            logger.warn("The {}={} setting in the consumer properties of the Kafka source for consumer group [{}] is "
                                + "not used: the source commits offsets itself, once events are processed.",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit, groupId);
        }
    }

    private static boolean enablesAutoCommit(@Nullable Object autoCommit) {
        return autoCommit != null && !"false".equalsIgnoreCase(String.valueOf(autoCommit));
    }
}
