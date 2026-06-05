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
package io.axoniq.workflow.runtime.api.execution.context;

import jakarta.annotation.Nonnull;

import java.util.Objects;

/**
 * Specification for a {@code ctx.migrateVersion(changeId, newVersion)} call. The {@code changeId} is carried
 * on {@link PrimitiveMetadata#stepName()}; {@code newVersion} is the value being recorded.
 *
 * @param primitiveMetadata metadata of the primitive ({@code stepName == changeId}).
 * @param newVersion        new workflow version to record (semver string).
 * @author Stefan Dragisic
 * @since 1.1.0
 */
public record VersionStepDefinition(
        @Nonnull PrimitiveMetadata primitiveMetadata,
        @Nonnull String newVersion
) {

    /**
     * Fails fast when a mandatory component is {@code null} — the {@code @Nonnull} annotation alone is
     * not enforced at runtime, so a user ignoring it is detected here at construction.
     */
    public VersionStepDefinition {
        Objects.requireNonNull(primitiveMetadata, "primitiveMetadata must not be null");
        Objects.requireNonNull(newVersion, "newVersion must not be null");
    }

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     */
    public VersionStepDefinition primitiveMetadata(@Nonnull PrimitiveMetadata primitiveMetadata) {
        return new VersionStepDefinition(primitiveMetadata, newVersion);
    }

    /**
     * Returns a copy of this definition with the provided step name. For migration steps the
     * step name carries the developer-chosen {@code changeId}.
     */
    public VersionStepDefinition stepName(@Nonnull String stepName) {
        return primitiveMetadata(primitiveMetadata.stepName(stepName));
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     */
    public VersionStepDefinition eventNameCustomizer(@Nonnull EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided new version (semver string).
     */
    public VersionStepDefinition newVersion(@Nonnull String newVersion) {
        return new VersionStepDefinition(primitiveMetadata, newVersion);
    }
}
