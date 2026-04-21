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
