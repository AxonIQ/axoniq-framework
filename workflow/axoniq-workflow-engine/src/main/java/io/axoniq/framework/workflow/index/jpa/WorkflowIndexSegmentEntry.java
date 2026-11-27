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
 * The position of the last token stored for one segment in the {@link JpaWorkflowIndex}, kept for the workflow
 * manager: a query for live workflows reads the events after the lowest of them. A segment claim reads after its own
 * token and ignores this row. A new position replaces every row whose range overlaps it, so a split or a merge leaves
 * none behind.
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
    }

    /**
     * Creates the position of the rows of one segment.
     *
     * @param workflowModule  the name of the workflow module
     * @param segmentFrom     the lowest segment key of the segment
     * @param segmentTo       the highest segment key of the segment
     * @param segmentPosition the global position in the event store of the last token stored for the segment
     */
    public WorkflowIndexSegmentEntry(String workflowModule, long segmentFrom, long segmentTo, long segmentPosition) {
        this.workflowModule = Objects.requireNonNull(workflowModule, "The workflow module must not be null.");
        this.segmentFrom = segmentFrom;
        this.segmentTo = segmentTo;
        this.segmentPosition = segmentPosition;
    }

    /**
     * Returns the lowest segment key of the segment.
     *
     * @return the lowest segment key of the segment
     */
    public long getSegmentFrom() {
        return segmentFrom;
    }

    /**
     * Returns the highest segment key of the segment.
     *
     * @return the highest segment key of the segment
     */
    public long getSegmentTo() {
        return segmentTo;
    }

    /**
     * Returns the global position in the event store of the last token stored for the segment.
     *
     * @return the position of the segment
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
