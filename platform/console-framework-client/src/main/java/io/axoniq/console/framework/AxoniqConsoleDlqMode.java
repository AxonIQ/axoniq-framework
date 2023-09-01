package io.axoniq.console.framework;

public enum AxoniqConsoleDlqMode {
    /**
     * All data of messages in the DLQ are available in the API.
     */
    FULL,
    /**
     * Limited data of messages in the DLQ are available in the API. Sensitive information is masked.
     * The sequence identifier is hashed through SHA256 to still enable processing actions.
     */
    MASKED,
    /**
     * No DLQ data is available in the API. Only the count in the overview. List actions will return an empty list.
     */
    NONE,
}
