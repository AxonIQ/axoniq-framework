/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.api;

import java.util.Collections;
import java.util.Map;

/**
 * Tags configuration labeling Axon Node represented by key-value pairs.
 *
 * @author Milan Savic
 * @since 4.2
 */
public class TagsConfiguration {

    /**
     * Tags represented by key-value pairs.
     */
    private final Map<String, String> tags;

    /**
     * The default constructor.
     */
    public TagsConfiguration() {
        this(Collections.emptyMap());
    }

    /**
     * Initializes tags configuration with key-value pairs.
     *
     * @param tags the map of {@link String} to {@link String} representing tags key-value pairs
     */
    public TagsConfiguration(Map<String, String> tags) {
        this.tags = tags;
    }

    /**
     * Gets tags.
     *
     * @return the map of {@link String} to {@link String} representing tags key-value pairs
     */
    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
}
