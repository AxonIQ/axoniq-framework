/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import org.axonframework.messaging.core.VersionedType;

import java.util.function.Predicate;

/**
 * A {@link SnapshotTransformation} refusing every snapshot stored at an identity its {@link #sourcePredicate()}
 * matches, so the entity is sourced in full instead.
 *
 * @param sourcePredicate the predicate deciding which stored identities to refuse
 * @param description     a description of the refused identities, for logging
 * @author John Hendrikx
 * @since 5.5.0
 */
record RebuildSnapshotTransformation(Predicate<VersionedType> sourcePredicate, String description)
        implements SnapshotTransformation {

    @Override
    public String toString() {
        return "rebuild " + description;
    }
}
