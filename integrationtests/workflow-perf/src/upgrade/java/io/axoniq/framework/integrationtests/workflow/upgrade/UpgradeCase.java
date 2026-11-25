/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow.upgrade;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

/**
 * The workflow and the events of the upgrade test, compiled once against the 5.4.1 release and once against the
 * current version. Both phases run the same body, so the current version replays what 5.4.1 recorded.
 * <p>
 * A case waits for its approval, runs the approve effect, sleeps, waits for its close and runs the archive effect. A
 * brief case expires when its approval does not arrive in time. Every effect is a row in PostgreSQL, so the test
 * counts how often an effect ran over both versions.
 *
 * @author Stefan Dragisic
 */
public final class UpgradeCase {

    public static final String MODULE = "upgrade";
    public static final String WORKFLOW = "UpgradeCase";
    public static final int SEGMENTS = 4;
    public static final Duration COOLDOWN = Duration.ofSeconds(20);
    public static final Duration BRIEF_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration LONG_TIMEOUT = Duration.ofDays(3);

    private UpgradeCase() {
    }

    public record CaseOpened(String caseId, boolean brief) {

    }

    public record CaseApproved(String caseId) {

    }

    public record CaseClosed(String caseId) {

    }

    /**
     * The body of the workflow. Its steps must not change between the versions, or the replay drifts.
     *
     * @param ctx the context of the workflow
     */
    public static void run(SimpleWorkflowContext ctx) {
        var caseId = String.valueOf(ctx.workflowPayload().get("caseId"));
        boolean brief = Boolean.parseBoolean(String.valueOf(ctx.workflowPayload().get("brief")));
        var byCase = associate(payloadProperty("caseId"), equalsTo(caseId));
        var approval = ctx.waitForEvent("awaitApproval", CaseApproved.class, byCase,
                                        step -> step.timeout(brief ? BRIEF_TIMEOUT : LONG_TIMEOUT));
        approval.await();
        if (!approval.success()) {
            ctx.awaitExecute("expire", String.class, () -> recordEffect(caseId, "expire"));
            return;
        }
        ctx.awaitExecute("approve", String.class, () -> recordEffect(caseId, "approve"));
        ctx.awaitSleep("cooldown", step -> step.timeout(COOLDOWN));
        ctx.awaitEvent("awaitClosed", CaseClosed.class, byCase, step -> step.timeout(LONG_TIMEOUT));
        ctx.awaitExecute("archive", String.class, () -> recordEffect(caseId, "archive"));
    }

    /**
     * Creates the table the effects are counted in, when it does not exist.
     */
    public static void createEffectTable() {
        execute("create table if not exists upgrade_effect (case_id varchar(64) not null, "
                        + "effect varchar(16) not null, version varchar(16) not null, "
                        + "ran_at timestamp not null default now())");
    }

    /**
     * Returns how often each effect of each case ran, keyed by case id and effect, as {@code case-1/approve}.
     *
     * @return the number of runs of each effect that ran
     */
    public static Map<String, Integer> effectCounts() {
        try (var connection = connection();
             var statement = connection.createStatement();
             var rows = statement.executeQuery(
                     "select case_id, effect, count(*) from upgrade_effect group by case_id, effect")) {
            var counts = new HashMap<String, Integer>();
            while (rows.next()) {
                counts.put(rows.getString(1) + "/" + rows.getString(2), rows.getInt(3));
            }
            return counts;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String recordEffect(String caseId, String effect) {
        execute("insert into upgrade_effect (case_id, effect, version) values ('" + caseId + "', '" + effect
                        + "', '" + System.getProperty("upgrade.version", "unknown") + "')");
        return effect;
    }

    private static void execute(String sql) {
        try (var connection = connection(); var statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static java.sql.Connection connection() throws SQLException {
        return DriverManager.getConnection(
                System.getProperty("perf.pg.url", "jdbc:postgresql://localhost:55450/perf"), "perf", "perf"
        );
    }
}
