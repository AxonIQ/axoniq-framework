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
