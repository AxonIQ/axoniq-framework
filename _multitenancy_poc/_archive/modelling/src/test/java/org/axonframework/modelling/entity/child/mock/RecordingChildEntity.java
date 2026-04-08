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

package org.axonframework.modelling.entity.child.mock;

import java.util.List;

public class RecordingChildEntity extends RecordingEntity<RecordingChildEntity> {

    private final String id;

    public RecordingChildEntity() {
        this("base-id", List.of());
    }

    public RecordingChildEntity(String id) {
        this(id, List.of());
    }

    public RecordingChildEntity(String id, List<String> evolves) {
        super("ChildEntity", evolves);
        this.id = id;
    }

    @Override
    protected RecordingChildEntity createNewInstance(List<String> evolves) {
        return new RecordingChildEntity(id, evolves);
    }

    public String getId() {
        return id;
    }
}
