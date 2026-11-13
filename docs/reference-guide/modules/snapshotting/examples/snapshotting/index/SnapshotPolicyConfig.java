/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package snapshotting.index;

import java.time.Duration;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;

public class SnapshotPolicyConfig {

    public void configure() {
        // tag::snapshot-policy[]
        SnapshotPolicy snapshotPolicy =
                SnapshotPolicy.afterEvents(5)
                              .or(SnapshotPolicy.whenSourcingTimeExceeds(
                                  Duration.ofMillis(500)
                              ));
        // end::snapshot-policy[]
    }
}
