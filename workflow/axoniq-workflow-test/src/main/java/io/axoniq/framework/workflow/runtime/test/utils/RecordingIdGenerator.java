/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import org.axonframework.common.annotation.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Recording id generator.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class RecordingIdGenerator implements IdGenerator {

    private final List<String> history = new ArrayList<>();

    @Override
    public String next() {
        String id = IdGenerator.super.next();
        history.add(id);
        return id;
    }

    /**
     * Retrieves id history.
     *
     * @return id history
     */
    public List<String> getHistory() {
        return Collections.unmodifiableList(history);
    }

    /**
     * Clears the generation history.
     */
    public void clear() {
        this.history.clear();
    }
}
