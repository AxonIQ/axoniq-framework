package io.axoniq.console.framework.api;

public enum DomainEventAccessMode {
    /**
     * Full access: payload is visible and loading domain state is supported.
     */
    FULL,

    /**
     * Payload is hidden (e.g., masked), but loading domain state is still supported.
     */
    LOAD_DOMAIN_STATE_ONLY,

    /**
     * Payload is visible, but loading domain state is not supported.
     */
    PREVIEW_PAYLOAD_ONLY,

    /**
     * No access: payload is hidden and loading domain state is not supported.
     */
    NONE
}