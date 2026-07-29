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

package io.axoniq.framework.messaging.multitenancy.eventstreaming;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.axoniq.framework.messaging.eventstreaming.MultiSourceTrackingToken;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A {@link TrackingToken} holding one position per tenant, positioning the merged read stream that feeds event
 * processors across all tenants.
 * <p>
 * It reuses a {@link MultiSourceTrackingToken} for the per-tenant positions, their aggregation, and their
 * serialization, and adds only what that token deliberately refuses: tolerance for a changing set of tenants. The set
 * of tenants grows and shrinks as tenants are added and removed, so when a processor compares a stored token (written
 * for an earlier set of tenants) with a live token (for the current set), the two carry different tenants. This token
 * tolerates that: its {@link #lowerBound}, {@link #upperBound}, {@link #covers}, and {@link #samePositionAs} operate
 * over the union of both tokens' tenants, treating a tenant present in only one of them as being at its beginning in
 * the other. A tenant added since the stored token was written therefore streams from its beginning. Confining the
 * tolerance here keeps the shared {@code MultiSourceTrackingToken}'s strict guardrail intact.
 * <p>
 * The fully qualified class name is written into every multi-tenant processor's token store when the token is
 * serialized. Renaming or moving this class therefore breaks existing token stores, so it can only be done together
 * with a token migration.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class MultiTenantTrackingToken implements TrackingToken {

    @JsonProperty("delegate")
    private final MultiSourceTrackingToken delegate;

    // Both the @JsonProperty and the @ConstructorProperties are required: converters resolving the constructor
    // parameter by its bean name rely on @ConstructorProperties, while those reading Jackson annotations rely on
    // @JsonProperty. The serialization round-trip test exercises both across every configured converter.
    @JsonCreator
    @ConstructorProperties({"delegate"})
    private MultiTenantTrackingToken(@JsonProperty("delegate") MultiSourceTrackingToken delegate) {
        this.delegate = Objects.requireNonNull(delegate, "The delegate token must not be null");
    }

    /**
     * Constructs a token holding the given {@code tenantTokens}, keyed by tenant id.
     *
     * @param tenantTokens the position per tenant, keyed by tenant id
     */
    public MultiTenantTrackingToken(Map<String, @Nullable TrackingToken> tenantTokens) {
        this(new MultiSourceTrackingToken(Objects.requireNonNull(tenantTokens, "The tenant tokens must not be null")));
    }

    /**
     * Returns an empty token, holding no tenant positions.
     *
     * @return an empty {@code MultiTenantTrackingToken}
     */
    public static MultiTenantTrackingToken empty() {
        return new MultiTenantTrackingToken(Map.of());
    }

    /**
     * Adapts the given {@code token} to a {@code MultiTenantTrackingToken}: an empty token when {@code null}, or the
     * token itself when it already is one.
     *
     * @param token the token to adapt
     * @return the token as a {@code MultiTenantTrackingToken}
     * @throws IllegalArgumentException if the token is a different, incompatible type
     */
    public static MultiTenantTrackingToken from(@Nullable TrackingToken token) {
        if (token == null) {
            return empty();
        }
        if (token instanceof MultiTenantTrackingToken multiTenantToken) {
            return multiTenantToken;
        }
        throw incompatibleToken(token);
    }

    /**
     * Returns a copy of this token with the given {@code tenantId} advanced to {@code newToken}.
     *
     * @param tenantId the tenant whose position is advanced
     * @param newToken the new position for the tenant
     * @return a token holding the advanced position for the tenant
     */
    public MultiTenantTrackingToken advancedTo(String tenantId, TrackingToken newToken) {
        Objects.requireNonNull(newToken, "The new token for the tenant must not be null");
        return new MultiTenantTrackingToken(delegate.advancedTo(tenantId, newToken));
    }

    /**
     * Returns the position of the given {@code tenantId}, or {@code null} when this token holds no position for it.
     *
     * @param tenantId the tenant to return the position of
     * @return the tenant's position, or {@code null} when absent
     */
    @Nullable
    public TrackingToken tokenForTenant(String tenantId) {
        return delegate.getTokenForStream(tenantId);
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        Map<String, @Nullable TrackingToken> otherTokens = tokensOf(other);
        Map<String, @Nullable TrackingToken> result = new HashMap<>();
        for (String tenantId : union(otherTokens)) {
            TrackingToken thisToken = delegate.getTokenForStream(tenantId);
            TrackingToken otherToken = otherTokens.get(tenantId);
            result.put(tenantId, thisToken == null || otherToken == null ? null : thisToken.lowerBound(otherToken));
        }
        return new MultiTenantTrackingToken(result);
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        Map<String, @Nullable TrackingToken> otherTokens = tokensOf(other);
        Map<String, @Nullable TrackingToken> result = new HashMap<>();
        for (String tenantId : union(otherTokens)) {
            result.put(tenantId, upperBoundOf(delegate.getTokenForStream(tenantId), otherTokens.get(tenantId)));
        }
        return new MultiTenantTrackingToken(result);
    }

    @Override
    public boolean covers(TrackingToken other) {
        Map<String, @Nullable TrackingToken> otherTokens = tokensOf(other);
        for (String tenantId : union(otherTokens)) {
            TrackingToken otherToken = otherTokens.get(tenantId);
            if (otherToken == null) {
                continue;
            }
            TrackingToken thisToken = delegate.getTokenForStream(tenantId);
            if (thisToken == null || !thisToken.covers(otherToken)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean samePositionAs(TrackingToken other) {
        Map<String, @Nullable TrackingToken> otherTokens = tokensOf(other);
        for (String tenantId : union(otherTokens)) {
            TrackingToken thisToken = delegate.getTokenForStream(tenantId);
            TrackingToken otherToken = otherTokens.get(tenantId);
            if (thisToken == null) {
                if (otherToken != null) {
                    return false;
                }
            } else if (otherToken == null || !thisToken.samePositionAs(otherToken)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public OptionalLong position() {
        return delegate.position();
    }

    private Set<String> union(Map<String, @Nullable TrackingToken> otherTokens) {
        Set<String> tenantIds = new LinkedHashSet<>(delegate.getTrackingTokens().keySet());
        tenantIds.addAll(otherTokens.keySet());
        return tenantIds;
    }

    @Nullable
    private static TrackingToken upperBoundOf(@Nullable TrackingToken thisToken, @Nullable TrackingToken otherToken) {
        if (thisToken == null) {
            return otherToken;
        }
        if (otherToken == null) {
            return thisToken;
        }
        return thisToken.upperBound(otherToken);
    }

    private static Map<String, @Nullable TrackingToken> tokensOf(TrackingToken other) {
        if (other instanceof MultiTenantTrackingToken multiTenantToken) {
            return multiTenantToken.delegate.getTrackingTokens();
        }
        throw incompatibleToken(other);
    }

    private static IllegalArgumentException incompatibleToken(TrackingToken token) {
        return new IllegalArgumentException("Incompatible token type provided: " + token.getClass().getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiTenantTrackingToken that = (MultiTenantTrackingToken) o;
        return delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    @Override
    public String toString() {
        return "MultiTenantTrackingToken{" + delegate + "}";
    }
}
