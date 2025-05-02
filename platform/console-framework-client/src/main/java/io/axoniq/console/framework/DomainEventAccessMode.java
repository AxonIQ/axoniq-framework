package io.axoniq.console.framework;

public enum DomainEventAccessMode {
    /**
     * Full access: payload is visible and LoadForAggregate is supported.
     */
    FULL,

    /**
     * Payload is hidden (e.g., masked or removed), but LoadForAggregate is still supported.
     */
    LOAD_SNAPSHOT_ONLY,

    /**
     * Payload is visible, but LoadForAggregate is not supported.
     */
    PREVIEW_PAYLOAD_ONLY,

    /**
     * No access: payload is hidden and LoadForAggregate is not supported.
     */
    NONE
}