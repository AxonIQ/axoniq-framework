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

import java.util.LinkedList;
import java.util.List;

public abstract class RecordingEntity<S extends RecordingEntity<S>> {

    private final String name;
    private final List<String> evolves;

    protected RecordingEntity(String name, List<String> evolves) {
        this.name = name;
        this.evolves = evolves;
    }

    public S evolve(String description) {
        LinkedList<String> list = new LinkedList<>(evolves);
        list.addFirst(description);
        return createNewInstance(list);
    }

    protected abstract S createNewInstance(List<String> evolves);

    public List<String> getEvolves() {
        return evolves;
    }

    @Override
    public String toString() {
        return name;
    }
}
