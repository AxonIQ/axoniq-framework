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

import org.jspecify.annotations.NonNull;

class DummyProvider implements UsagePropertyProvider {

    private final Boolean disabled;
    private final String url;
    private final int priority;

    DummyProvider(Boolean disabled, String url, int priority) {
        this.disabled = disabled;
        this.url = url;
        this.priority = priority;
    }

    @Override
    public @NonNull Boolean getDisabled() {
        return disabled;
    }

    @Override
    public @NonNull String getUrl() {
        return url;
    }

    @Override
    public int priority() {
        return priority;
    }
}
