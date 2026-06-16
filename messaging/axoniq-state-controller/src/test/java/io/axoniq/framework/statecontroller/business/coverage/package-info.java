/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

/**
 * End-to-end coverage of the Dynamic Consistency Boundary (DCB) tagging-drift guard, exercised through Axon
 * Framework's {@code AxonTestFixture} so the whole command path runs against a real in-memory event store.
 * <p>
 * The DCB optimistic lock an accepted decision relies on is only sound when every appended event lands inside a
 * consistency boundary the decision actually read. The read tag (derived from {@code history.of("account", id)})
 * and the write tag (derived from the configured {@code TagResolver} — typically {@code @EventTag} on the event)
 * are wired independently and can silently drift apart. The guard closes that gap: an accepted event covered by
 * no read scope fails fast rather than committing against the wrong (or empty) surface.
 * <p>
 * The fixtures here use the default configuration the fixture wires (an {@code AnnotationBasedTagResolver} both
 * registered as the {@code TagResolver} component and used to tag appended events), so the tags the guard computes
 * equal the tags the append commits. They prove the guard fires on drift, passes on correct tagging, stays
 * disabled for an unconditional (no-read) accept, and never touches the legacy {@code @StateController} path.
 */
@NullMarked
package io.axoniq.framework.statecontroller.business.coverage;

import org.jspecify.annotations.NullMarked;
