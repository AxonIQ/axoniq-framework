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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 * One node of the S5 multi-JVM fencing rig: a real JVM running one workflow engine over a shared Axon Server DCB
 * store with its own in-memory token store. Role A publishes the start event; both roles run the workflow body.
 * Progress markers go to stdout so the orchestrating test can SIGSTOP/SIGCONT this process at the right moments.
 *
 * <p>Args: role(A|B) host grpcPort workflowId actionSleepMs</p>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class S5NodeMain {

    private S5NodeMain() {
    }

    public static void main(String[] args) throws Exception {
        String role = args[0];
        String host = args[1];
        int grpcPort = Integer.parseInt(args[2]);
        String workflowId = args[3];
        long actionSleepMs = Long.parseLong(args[4]);

        var connection = AxonServerConnectionFactory
                .forClient("s5-" + role + "-" + System.nanoTime())
                .routingServers(new ServerAddress(host, grpcPort))
                .build()
                .connect("default");
        var store = new AxonServerEventStorageEngine(connection, DcbFencingBackends.converter());

        var node = DcbFencingBackends.startNode(store, "s5-node-" + role, new SlowWorkflow(role, actionSleepMs));
        out("NODE-UP role=" + role + " pid=" + ProcessHandle.current().pid());

        if (role.equals("A")) {
            node.publish(new StartSlow(workflowId));
            out("START-PUBLISHED id=" + workflowId);
        }

        // Stay alive until the orchestrator kills this process.
        Thread.sleep(Long.MAX_VALUE);
    }

    // The orchestrator reads this process protocol from the child stdout; a logger would reformat it.
    private static final PrintStream STDOUT = new PrintStream(new FileOutputStream(FileDescriptor.out), true);

    static void out(String line) {
        STDOUT.println(line);
    }

    /** Start event; idProperty binds the workflow instance id. */
    public record StartSlow(String id) {

    }

    /** Wake-up signal the parked workflow waits for, correlated by workflow id. */
    public record GoSignal(String id) {

    }

    /**
     * prepare (quick) → park on GoSignal → shipFinal (quick). The park is where the claim moves: the stale node is
     * frozen while parked, the takeover node restores the parked instance, and the GoSignal races both nodes into
     * shipFinal. A rejected shipFinal STARTED must mean the shipFinal action never ran on that node (CL-3).
     */
    public static final class SlowWorkflow {

        private final String role;

        public SlowWorkflow(String role, long ignoredActionSleepMs) {
            this.role = role;
        }

        @Workflow(
                workflowName = "S5SlowWorkflow",
                idProperty = "id",
                startOnEventClass = StartSlow.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            out("BODY-RUN role=" + role + " id=" + ctx.workflowId());
            ctx.awaitExecute("prepare", Boolean.class, () -> {
                out("PREPARE-RUN role=" + role + " id=" + ctx.workflowId());
                return true;
            });
            out("WAITING role=" + role + " id=" + ctx.workflowId());
            ctx.awaitEvent("goSignal", GoSignal.class,
                           io.axoniq.framework.workflow.runtime.association.Associations.associate(
                                   io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever
                                           .payloadProperty("id"),
                                   io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo(ctx.workflowId())),
                           step -> step.timeout(java.time.Duration.ofMinutes(10)));
            ctx.awaitExecute("shipFinal", Boolean.class, () -> {
                out("FINAL-RUN role=" + role + " id=" + ctx.workflowId());
                return true;
            });
            out("BODY-END role=" + role + " id=" + ctx.workflowId());
        }
    }
}
