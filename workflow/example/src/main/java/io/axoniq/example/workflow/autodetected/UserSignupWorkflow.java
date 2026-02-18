package io.axoniq.example.workflow.autodetected;

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.NotificationService;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.example.workflow.fixture.UserService;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.Workflow;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class UserSignupWorkflow {

    @Workflow(
            workflowName = "UserSignup",
            idProperty = "id",
            startOn = RegistrationReceivedEvent.class,
            workflowNamespace = "io.axoniq.dsl.wf.autodetected"

    )
    public void execute(@Nonnull SimpleWorkflowContext ctx) {

        Logger logger = LoggerFactory.getLogger(UserSignupWorkflow.class);

        logger.info("User signup workflow started at {} for {}", Instant.now(), ctx.getPayload());

        // -> start
        var success = ctx.awaitExecute("createUser", Boolean.class, UserService::createUser);
        if (!success) {
            return;
        }

        ctx.awaitExecute("activateUser", ctx.getPayload(), UserService::activateUser, Duration.ofSeconds(10));

        /**
         var a1 = ctx.executeWithResult("activateUser", payload().set("id", "id1").getValues(), UserService::activateUser, Duration.ofSeconds(10));
         var a2 = ctx.executeWithResult("activateUser2", payload().set("id", "id2").getValues(), UserService::activateUser, Duration.ofSeconds(10));
         all(a1, a2).isSuccess();
         */


        ctx.awaitExecute("sendWelcomeEmail", NotificationService::sendEmail);
        ctx.block("waitASecond", Duration.ofSeconds(1L));

        var magic = ctx.awaitEvent("waitForMagicToHappen", MagicHappenedEvent.class, Duration.ofSeconds(5));
        ctx.addPayload(magic);

        logger.info("Magic happened because of the magician {}", magic.magician());
        // -> end

        logger.info("User signup workflow ended at {} for {}", Instant.now(), ctx.getPayload());
    }
}
