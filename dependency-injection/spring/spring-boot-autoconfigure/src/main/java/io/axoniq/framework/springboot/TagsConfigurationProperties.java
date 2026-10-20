/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import io.axoniq.framework.axonserver.connector.api.TagsConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring Configuration properties for {@link TagsConfiguration}.
 *
 * @author Milan Savic
 * @since 4.2.0
 */
@ConfigurationProperties(prefix = "axon")
public class TagsConfigurationProperties {

    /**
     * Tags represented by key-value pairs.
     */
    private Map<String, String> tags = new HashMap<>();

    /**
     * Gets tags.
     *
     * @return the map of {@link String} to {@link String} representing tags key-value pairs
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * Sets tags.
     *
     * @param tags the map of {@link String} to {@link String} representing tags key-value pairs
     */
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public TagsConfiguration toTagsConfiguration() {
        return new TagsConfiguration(tags);
    }
}
