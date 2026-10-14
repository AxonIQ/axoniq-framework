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

package io.axoniq.framework.springcloud.shared;

import io.axoniq.framework.springcloud.discovery.MemberAdvertisement;
import io.axoniq.framework.springcloud.discovery.ServiceInstanceKey;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * What every instance discovery reports last answered when asked which member it is and what it handles, in
 * discovery's own order.
 * <p>
 * Immutable: every change returns a new {@code InstanceAnswers}, so the {@link SpringCloudMemberDiscovery} holding
 * one can replace it as a whole, and the rules deciding who is a member can be exercised without any discovery
 * round, thread or timer.
 * <p>
 * {@link Internal} because it only exists to keep that bookkeeping out of the {@link SpringCloudMemberDiscovery}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class InstanceAnswers {

    private static final Logger logger = LoggerFactory.getLogger(InstanceAnswers.class);

    /**
     * The answers known before any instance was asked.
     */
    static final InstanceAnswers NONE = new InstanceAnswers(Map.of());

    private final Map<ServiceInstanceKey, InstanceAnswer> answers;

    private InstanceAnswers(Map<ServiceInstanceKey, InstanceAnswer> answers) {
        this.answers = answers;
    }

    /**
     * Returns every instance an answer is known for, as discovery last reported it.
     *
     * @return the instances an answer is known for, in discovery's own order
     */
    List<ServiceInstance> instances() {
        return answers.values().stream().map(InstanceAnswer::instance).toList();
    }

    /**
     * Returns the keys of every instance an answer is known for.
     *
     * @return the keys of the instances an answer is known for
     */
    Set<ServiceInstanceKey> keys() {
        return Set.copyOf(answers.keySet());
    }

    /**
     * Returns those of the given {@code instances} that are due to be asked: those that have not answered, whose
     * answer was forgotten, or whose answer is at least {@code refreshInterval} old.
     *
     * @param instances       the instances to select those due from
     * @param now             the current {@link System#nanoTime()}
     * @param refreshInterval how long an answer is taken as current
     * @return the given instances that are due to be asked, in the given order
     */
    List<ServiceInstance> due(List<ServiceInstance> instances, long now, Duration refreshInterval) {
        return instances.stream()
                        .filter(instance -> {
                            InstanceAnswer answer = answers.get(ServiceInstanceKey.of(instance));
                            return answer == null
                                    || answer.advertisement() == null
                                    || now - answer.answeredAt() >= refreshInterval.toNanos();
                        })
                        .toList();
    }

    /**
     * Returns the answers of exactly the given {@code instances}, taking each from {@code asked} when it was just
     * asked, and keeping what it answered before otherwise.
     * <p>
     * Instances not among the given {@code instances} are dropped, as discovery no longer reports them. Each instance
     * is kept as given, which may carry newer metadata than what discovery reported before.
     *
     * @param instances the instances discovery currently reports, in discovery's own order
     * @param asked     the answers of the instances that were just asked
     * @return the answers of the given {@code instances}
     */
    InstanceAnswers merge(List<ServiceInstance> instances, Map<ServiceInstanceKey, InstanceAnswer> asked) {
        Map<ServiceInstanceKey, InstanceAnswer> merged = new LinkedHashMap<>();
        for (ServiceInstance instance : instances) {
            ServiceInstanceKey key = ServiceInstanceKey.of(instance);
            InstanceAnswer answer = Objects.requireNonNullElseGet(
                    asked.get(key),
                    () -> answers.getOrDefault(key, InstanceAnswer.unanswered(instance))
            );
            merged.putIfAbsent(key, answer.reportedAs(instance));
        }
        return new InstanceAnswers(merged);
    }

    /**
     * Returns these answers with what the given {@code member} answered forgotten, so that it is not a member again
     * until it answers again.
     *
     * @param member the member whose answers to forget
     * @return these answers without those of the given {@code member}
     */
    InstanceAnswers forget(Member member) {
        Map<ServiceInstanceKey, InstanceAnswer> remaining = new LinkedHashMap<>(answers);
        remaining.replaceAll((key, answer) -> answer.isFrom(member) ? answer.forgotten() : answer);
        return new InstanceAnswers(remaining);
    }

    /**
     * Derives the members of the cluster from these answers.
     * <p>
     * Answers are taken in discovery's own order. Instances answering with the same node id are one member, reached
     * at the first of them, so every member of the cluster picks the same one. Instances that have not answered, or
     * whose endpoint is not known yet, are left out.
     * <p>
     * The instance answering with the given {@code localNodeId} is this application, which is not one of the
     * {@link Members#remote() remote} members. This application is part of its own ring when discovery reports it,
     * which is what every other member goes by as well. A member discovery left out stays out, so that this
     * application does not route commands to itself that the rest of the cluster routes elsewhere. Only when discovery
     * reports nothing at all does this application keep itself, since it can handle its own commands while it waits
     * for discovery to catch up.
     *
     * @param localNodeId      the node id of this application
     * @param endpointResolver resolves the endpoint an instance is reached at, or {@code null} when it has none yet
     * @return the members of the cluster according to these answers
     */
    Members members(String localNodeId, Function<ServiceInstance, @Nullable URI> endpointResolver) {
        Map<String, Member> membersByNodeId = new LinkedHashMap<>();
        Map<Member, MemberCapabilities> remote = new LinkedHashMap<>();
        boolean reportsLocal = false;
        for (InstanceAnswer answer : answers.values()) {
            MemberAdvertisement advertisement = answer.advertisement();
            if (advertisement == null) {
                continue;
            }
            if (advertisement.nodeId().equals(localNodeId)) {
                reportsLocal = true;
                continue;
            }
            Member known = membersByNodeId.get(advertisement.nodeId());
            if (known != null) {
                logger.debug("ServiceInstance [{}] is member [{}], which is already reached at [{}].",
                             ServiceInstanceKey.of(answer.instance()), advertisement.nodeId(), known.endpoint());
                continue;
            }
            URI endpoint = endpointResolver.apply(answer.instance());
            if (endpoint == null) {
                // Only possible for an instance that answered without reporting where it is, such as one a discovery
                // implementation has not finished registering.
                continue;
            }
            Member member = new Member(advertisement.nodeId(), endpoint, false);
            membersByNodeId.put(advertisement.nodeId(), member);
            remote.put(member, advertisement.capabilities());
        }
        return new Members(remote, reportsLocal || answers.isEmpty());
    }

    /**
     * The members of the cluster according to the known answers.
     *
     * @param remote        every member other than this application, and what each of them handles
     * @param includesLocal whether this application is part of the ring
     */
    record Members(Map<Member, MemberCapabilities> remote, boolean includesLocal) {

    }

    /**
     * What one instance discovery reports last answered, and when.
     *
     * @param instance      the instance as discovery last reported it
     * @param advertisement what the instance answered, or {@code null} when it has not answered, or its answer was
     *                      forgotten because the member could not be reached
     * @param answeredAt    the {@link System#nanoTime()} at which the instance was asked
     */
    record InstanceAnswer(ServiceInstance instance, @Nullable MemberAdvertisement advertisement, long answeredAt) {

        private static InstanceAnswer unanswered(ServiceInstance instance) {
            return new InstanceAnswer(instance, null, 0);
        }

        private InstanceAnswer reportedAs(ServiceInstance reported) {
            return new InstanceAnswer(reported, advertisement, answeredAt);
        }

        private InstanceAnswer forgotten() {
            return new InstanceAnswer(instance, null, answeredAt);
        }

        private boolean isFrom(Member member) {
            return advertisement != null && advertisement.nodeId().equals(member.name());
        }
    }
}
