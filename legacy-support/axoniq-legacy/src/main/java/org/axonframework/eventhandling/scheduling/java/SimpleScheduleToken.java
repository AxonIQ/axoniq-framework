/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling.java;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.jspecify.annotations.Nullable;

import java.beans.ConstructorProperties;
import java.io.Serial;
import java.util.Objects;

/**
 * {@link ScheduleToken} for events scheduled using the {@link SimpleEventScheduler}. Axon Framework 4's
 * {@code AxonServerEventScheduler} returned this token as well, holding the identifier Axon Server assigned.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SimpleScheduleToken implements ScheduleToken {

    @Serial
    private static final long serialVersionUID = -8118223354702247016L;
    private final String tokenId;

    /**
     * Creates a SimpleScheduleToken with the given {@code tokenId}.
     *
     * @param tokenId the identifier referencing the scheduled task
     */
    @JsonCreator
    @ConstructorProperties({"tokenId"})
    public SimpleScheduleToken(@JsonProperty("tokenId") String tokenId) {
        this.tokenId = tokenId;
    }

    /**
     * Returns the identifier of the scheduled task.
     *
     * @return the identifier of the scheduled task
     */
    public String getTokenId() {
        return tokenId;
    }

    @Override
    public String toString() {
        return "SimpleScheduleToken{" +
                "tokenId='" + tokenId + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenId);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SimpleScheduleToken other = (SimpleScheduleToken) obj;
        return Objects.equals(this.tokenId, other.tokenId);
    }
}
