package distributedmessaging.distributedquerybus.disableshortcut;

import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;

class DisableLocalShortcut {

    void disable() {
        // tag::disable-local-shortcut[]
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT
                .preferLocalQueryHandler(false);  // Force all queries through connector
        // end::disable-local-shortcut[]
    }
}
