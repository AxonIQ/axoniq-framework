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

package org.axonframework.update.configuration;

import org.axonframework.common.annotation.Internal;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Combines multiple {@link UsagePropertyProvider} instances into a single provider.
 * It will return the first non-null value for each property from the list of providers, sorted by their priority.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Internal
public class HierarchicalUsagePropertyProvider implements UsagePropertyProvider {

    private final List<UsagePropertyProvider> providers;

    /**
     * Creates a new {@code HierarchicalUsagePropertyProvider} with the given list of providers.
     * The providers will be sorted by their priority in descending order, meaning the highest priority provider
     * will be checked first.
     *
     * @param providers The list of {@link UsagePropertyProvider} instances to combine.
     */
    public HierarchicalUsagePropertyProvider(List<UsagePropertyProvider> providers) {
        Objects.requireNonNull(providers, "The providers may not be null.");
        this.providers = providers.stream()
                                  .sorted(Comparator.comparingInt(UsagePropertyProvider::priority).reversed())
                                  .toList();
    }

    @Override
    public Boolean getDisabled() {
        return providers.stream()
                        .map(UsagePropertyProvider::getDisabled)
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(false);
    }

    @Override
    public String getUrl() {
        return providers.stream().map(UsagePropertyProvider::getUrl)
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse("");
    }

    @Override
    public int priority() {
        // Does not matter for the combined provider, as it is not used directly.
        return 0;
    }
}
