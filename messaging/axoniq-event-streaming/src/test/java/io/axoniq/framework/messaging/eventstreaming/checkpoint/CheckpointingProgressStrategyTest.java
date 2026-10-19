/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;

import java.util.List;

/**
 * Test class running the {@link CheckpointingProgressStrategyTestSuite} against the framework's
 * {@link CheckpointingProgressStrategy}.
 *
 * @author Allard Buijze
 */
class CheckpointingProgressStrategyTest extends CheckpointingProgressStrategyTestSuite {

    @Override
    protected SegmentProgressStrategyFactory checkpointingStrategy(boolean autoCheckpointing,
                                                                   List<Checkpointing> participants) {
        return context -> new CheckpointingProgressStrategy(context, participants, autoCheckpointing);
    }
}
