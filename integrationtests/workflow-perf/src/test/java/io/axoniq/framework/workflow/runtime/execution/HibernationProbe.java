/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

/**
 * Reads how many workflows an engine holds hibernated. Lives in the engine's package because the hibernation support
 * is package-private; the load test only reads, it never changes engine state.
 *
 * @author Stefan Dragisic
 */
public final class HibernationProbe {

    private HibernationProbe() {
    }

    /**
     * Returns the number of workflows the given engine holds hibernated, or 0 when hibernation is disabled.
     *
     * @param engine the engine to inspect
     * @return the number of hibernated workflows
     */
    public static int hibernatedCount(WorkflowEngine engine) {
        return hibernatedIds(engine).size();
    }

    /**
     * Returns the ids of the workflows the given engine holds hibernated, or none when hibernation is disabled.
     *
     * @param engine the engine to inspect
     * @return the ids of the hibernated workflows
     */
    /**
     * Returns the open waits of each workflow the given engine holds hibernated, by workflow id, or none when
     * hibernation is disabled.
     *
     * @param engine the engine to inspect
     * @return the step names of the open waits of each hibernated workflow
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Set<String>> openWaitsOfHibernated(WorkflowEngine engine) {
        var support = engine.hibernation;
        if (support == null) {
            return Map.of();
        }
        try {
            Field field = WorkflowEngineHibernationSupport.class.getDeclaredField("hibernations");
            field.setAccessible(true);
            var result = new java.util.HashMap<String, Set<String>>();
            for (var entry : ((Map<String, ?>) field.get(support)).entrySet()) {
                var accessor = entry.getValue().getClass().getDeclaredMethod("openWaits");
                accessor.setAccessible(true);
                result.put(entry.getKey(), Set.copyOf((Set<String>) accessor.invoke(entry.getValue())));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Set<String> hibernatedIds(WorkflowEngine engine) {
        var support = engine.hibernation;
        if (support == null) {
            return Set.of();
        }
        try {
            Field field = WorkflowEngineHibernationSupport.class.getDeclaredField("hibernations");
            field.setAccessible(true);
            return Set.copyOf(((Map<String, ?>) field.get(support)).keySet());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
