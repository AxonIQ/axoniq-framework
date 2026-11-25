/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;

import java.io.Serializable;
import java.util.Objects;

/**
 * The position the rows of one segment in the {@link JpaWorkflowIndex} are correct at: every lifecycle and wait event
 * of the segment before it is applied to them. A segment claim reads the events from there.
 * <p>
 * The row is keyed by the range of segment keys of the segment. A segment without a row has rows that started with an
 * empty index, or that a split or a merge replaced; its claim sources instead.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Entity
@IdClass(WorkflowIndexSegmentEntry.PK.class)
public class WorkflowIndexSegmentEntry {

    @Id
    private String workflowModule;

    @Id
    private long segmentFrom;

    @Id
    private long segmentTo;

    @Basic(optional = false)
    private long segmentPosition;

    /**
     * Constructor required by JPA. Do not use.
     */
    protected WorkflowIndexSegmentEntry() {
        // required by JPA
    }

    /**
     * Creates the position of the rows of one segment.
     *
     * @param workflowModule  the name of the workflow module
     * @param segmentFrom     the lowest segment key of the segment
     * @param segmentTo       the highest segment key of the segment
     * @param segmentPosition the global position in the event store the rows of the segment are correct at
     */
    public WorkflowIndexSegmentEntry(String workflowModule, long segmentFrom, long segmentTo, long segmentPosition) {
        this.workflowModule = Objects.requireNonNull(workflowModule, "The workflow module must not be null.");
        this.segmentFrom = segmentFrom;
        this.segmentTo = segmentTo;
        this.segmentPosition = segmentPosition;
    }

    /**
     * Returns the global position in the event store the rows of the segment are correct at.
     *
     * @return the position of the rows of the segment
     */
    public long getSegmentPosition() {
        return segmentPosition;
    }

    /**
     * The primary key of a {@link WorkflowIndexSegmentEntry}: the workflow module and the range of the segment.
     */
    public static class PK implements Serializable {

        private String workflowModule;
        private long segmentFrom;
        private long segmentTo;

        /**
         * Constructor required by JPA. Do not use.
         */
        public PK() {
            // required by JPA
        }

        /**
         * Creates the key of the position of the given segment.
         *
         * @param workflowModule the name of the workflow module
         * @param segmentFrom    the lowest segment key of the segment
         * @param segmentTo      the highest segment key of the segment
         */
        public PK(String workflowModule, long segmentFrom, long segmentTo) {
            this.workflowModule = workflowModule;
            this.segmentFrom = segmentFrom;
            this.segmentTo = segmentTo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PK that = (PK) o;
            return segmentFrom == that.segmentFrom
                    && segmentTo == that.segmentTo
                    && Objects.equals(workflowModule, that.workflowModule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workflowModule, segmentFrom, segmentTo);
        }
    }
}
