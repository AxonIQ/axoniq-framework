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
 *
 * @param storesWithToken whether the rows are stored in the transaction of the token store, so they commit with the
 *                        token; {@code false} when the index lives in another database
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public record JpaWorkflowIndexConfiguration(boolean storesWithToken) {

    /**
     * The default configuration, which stores the rows in the transaction of the token store.
     */
    public static final JpaWorkflowIndexConfiguration DEFAULT = new JpaWorkflowIndexConfiguration(true);

    /**
     * Sets whether the rows are stored in the transaction of the token store. Defaults to {@code true}. With
     * {@code false}, the engine keeps no wake times, so a segment claim loads every running workflow.
     *
     * @param storesWithToken whether rows and token are committed in one transaction
     * @return a copy of this configuration with the given setting
     */
    public JpaWorkflowIndexConfiguration storesWithToken(boolean storesWithToken) {
        return new JpaWorkflowIndexConfiguration(storesWithToken);
    }
}
