/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

/**
 * Configuration of a {@link JpaWorkflowIndex}.
 * <p>
 * Example usage:
 * <pre>{@code
 * JpaWorkflowIndexConfiguration.DEFAULT.storesWithToken(false);
 * }</pre>
 *
 * @param storesWithToken whether the index is stored through the transaction of the token store, so its rows and the
 *                        token are committed together. Set it to {@code false} when the index lives in another database
 *                        than the token store
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public record JpaWorkflowIndexConfiguration(boolean storesWithToken) {

    /**
     * The default configuration: the index is stored through the transaction of the token store.
     */
    public static final JpaWorkflowIndexConfiguration DEFAULT = new JpaWorkflowIndexConfiguration(true);

    /**
     * Sets whether the index is stored through the transaction of the token store. Defaults to {@code true}.
     * <p>
     * With {@code false}, the rows are stored in a transaction of their own, before the token. The engine then stores
     * no wake time for a hibernated workflow, so a segment claim loads every running workflow.
     *
     * @param storesWithToken whether rows and token are committed in one transaction
     * @return a copy of this configuration with the given setting
     */
    public JpaWorkflowIndexConfiguration storesWithToken(boolean storesWithToken) {
        return new JpaWorkflowIndexConfiguration(storesWithToken);
    }
}
