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

package org.axonframework.test.fixture.sampledomain;

import org.axonframework.eventsourcing.annotation.EventSourcingHandler;

/**
 * Event-sourced Student model
 */
public class Student {

    private String id;
    private String name;
    private Integer changes = 0;

    public Student(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Integer getChanges() {
        return changes;
    }

    @EventSourcingHandler
    public void on(StudentNameChangedEvent event) {
        this.name = event.name();
        this.changes = event.change();
    }
}
