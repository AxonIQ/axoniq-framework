/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.usersignup;

import io.axoniq.example.workflow.MagicHappenedEvent;
import io.axoniq.example.workflow.NotificationService;
import io.axoniq.example.workflow.UserService;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class UserSignupWorkflow {

  public void execute(@Nonnull SimpleWorkflowContext ctx) {

    Logger logger = LoggerFactory.getLogger(UserSignupWorkflow.class);

    logger.info("User signup workflow started at {} for {}", Instant.now(), ctx.getPayload());

    // -> start
    var success = ctx.execute("createUser", Boolean.class, UserService::createUser);
    if (!success) {
      return;
    }

    ctx.execute("activateUser", ctx.getPayload(), UserService::activateUser, Duration.ofSeconds(10));

    /**
     var a1 = ctx.executeWithResult("activateUser", payload().set("id", "id1").getValues(), UserService::activateUser, Duration.ofSeconds(10));
     var a2 = ctx.executeWithResult("activateUser2", payload().set("id", "id2").getValues(), UserService::activateUser, Duration.ofSeconds(10));
     all(a1, a2).isSuccess();
     */


    ctx.execute("sendWelcomeEmail", NotificationService::sendEmail);
    ctx.wait("waitASecond", Duration.ofSeconds(1L));

    var magic = ctx.waitForEvent("waitForMagicToHappen", MagicHappenedEvent.class, Duration.ofSeconds(5));
    ctx.addPayload(magic);

    logger.info("Magic happened because of the magician {}", magic.magician());
    // -> end

    logger.info("User signup workflow ended at {} for {}", Instant.now(), ctx.getPayload());
  }
}
