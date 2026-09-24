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

package io.axoniq.framework.springcloud.transport;

import org.axonframework.common.AxonNonTransientException;
import org.axonframework.conversion.ConversionException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link QueryConverter} moves a query and its answers between the framework's messages and the form
 * members send each other.
 *
 * @author Allard Buijze
 */
class QueryConverterTest {

    private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
    private static final MessageType RESPONSE_TYPE = new MessageType("university.Course", "1.0.0");
    private static final String PAYLOAD = "{\"id\":\"course-1\"}";

    private static QueryMessage query(Map<String, String> metadata) {
        return new GenericQueryMessage(
                new GenericMessage("query-1", FIND_COURSE_TYPE, PAYLOAD, metadata), null
        );
    }

    private static QueryResponseMessage response() {
        return new GenericQueryResponseMessage(
                new GenericMessage("response-1", RESPONSE_TYPE, PAYLOAD, Map.of("tenant", "acme"))
        );
    }

    @Nested
    class SendingAQuery {

        @Test
        void carriesEverythingNeededToRebuildTheQuery() {
            // when
            QueryDispatchRequest request = QueryConverter.convertQueryMessage(query(Map.of("tenant", "acme")));

            // then
            assertThat(request.identifier()).isEqualTo("query-1");
            assertThat(request.type()).isEqualTo(FIND_COURSE_TYPE.toString());
            assertThat(request.metadata()).containsEntry("tenant", "acme");
        }

        @Test
        void rebuildsTheQueryTheMemberSent() {
            // given
            QueryDispatchRequest request = QueryConverter.convertQueryMessage(query(Map.of("tenant", "acme")));

            // when
            QueryMessage received = QueryConverter.convertRequest(request, null);

            // then the query arrives as the member sent it, payload included
            assertThat(received.identifier()).isEqualTo("query-1");
            assertThat(received.type()).isEqualTo(FIND_COURSE_TYPE);
            assertThat(received.metadata()).containsEntry("tenant", "acme");
            assertThat(received.payload()).isEqualTo(PAYLOAD);
        }

        @Test
        void carriesMetadataWithoutAValue() {
            // given metadata permits null values, which the wire format has to tolerate
            Map<String, String> metadata = new HashMap<>();
            metadata.put("tenant", null);

            // when
            QueryMessage received =
                    QueryConverter.convertRequest(QueryConverter.convertQueryMessage(query(metadata)), null);

            // then
            assertThat(received.metadata()).containsEntry("tenant", null);
        }

        @Test
        void refusesAPayloadThatWasNeverConverted() {
            // given a query whose payload is still in its domain form, with no converter to write it as text
            QueryMessage unconverted = new GenericQueryMessage(
                    new GenericMessage("query-1", FIND_COURSE_TYPE, Map.of("id", "course-1"), Map.of()), null
            );

            // when / then
            assertThatThrownBy(() -> QueryConverter.convertQueryMessage(unconverted))
                    .isInstanceOf(ConversionException.class)
                    .hasMessageContaining("java.lang.String");
        }
    }

    @Nested
    class AnsweringAQuery {

        @Test
        void namesTheQueryEachResponseAnswers() {
            // when
            QueryDispatchResponse event = QueryConverter.convertResponseMessage(response(), "query-1");

            // then
            assertThat(event.identifier()).isEqualTo("response-1");
            assertThat(event.requestIdentifier()).isEqualTo("query-1");
            assertThat(event.type()).isEqualTo(RESPONSE_TYPE.toString());
        }

        @Test
        void rebuildsTheResponseTheMemberSent() {
            // given
            QueryDispatchResponse event = QueryConverter.convertResponseMessage(response(), "query-1");

            // when
            QueryResponseMessage received = QueryConverter.convertResponse(event, null);

            // then
            assertThat(received.identifier()).isEqualTo("response-1");
            assertThat(received.type()).isEqualTo(RESPONSE_TYPE);
            assertThat(received.metadata()).containsEntry("tenant", "acme");
            assertThat(received.payload()).isEqualTo(PAYLOAD);
        }
    }

    @Nested
    class ReportingAFailure {

        @Test
        void keepsAMissingHandlerRetryable() {
            // given
            QueryDispatchFailure event = QueryConverter.convertErrorResult(
                    new NoHandlerForQueryException("Still starting up."), "query-1", "node-b", null
            );

            // when
            RuntimeException rebuilt = QueryConverter.convertError(event);

            // then the member that asked may usefully try another member, or the same one later
            assertThat(event.errorCode()).isEqualTo(QueryErrorCode.NO_HANDLER_FOR_QUERY);
            assertThat(rebuilt).isInstanceOf(NoHandlerForQueryException.class);
        }

        @Test
        void marksAFailureThatWillRepeatAsNotWorthRetrying() {
            // given
            QueryDispatchFailure event = QueryConverter.convertErrorResult(
                    new UnreadableQueryException("Cannot read it.", new IllegalStateException()),
                    "query-1", "node-b", null
            );

            // when
            RuntimeException rebuilt = QueryConverter.convertError(event);

            // then
            assertThat(event.errorCode()).isEqualTo(QueryErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR);
            assertThat(rebuilt).isInstanceOf(QueryExecutionException.class);
            assertThat(rebuilt.getCause()).isInstanceOf(AxonNonTransientException.class);
        }

        @Test
        void pointsAtTheMemberTheFailureOccurredOn() {
            // when
            QueryDispatchFailure event = QueryConverter.convertErrorResult(
                    new QueryExecutionException("The course store is unavailable.", null),
                    "query-1", "node-b", null
            );

            // then
            assertThat(event.errorOrigin()).isEqualTo("node-b");
            assertThat(QueryConverter.convertError(event)).hasMessageContaining("node-b");
        }

        @Test
        void carriesTheDetailsAHandlerAttached() {
            // given a handler that rejected the query with details of its own
            byte[] details = "{\"reason\":\"closed\"}".getBytes(StandardCharsets.UTF_8);
            QueryDispatchFailure event = QueryConverter.convertErrorResult(
                    new QueryExecutionException("Rejected.", null, details), "query-1", "node-b", null
            );

            // when
            RuntimeException rebuilt = QueryConverter.convertError(event);

            // then
            assertThat(HandlerExecutionException.resolveDetails(rebuilt)).contains(details);
        }

        @Test
        void describesTheChainOfCausesBehindAFailure() {
            // given
            QueryDispatchFailure event = QueryConverter.convertErrorResult(
                    new QueryExecutionException("Outer.", new IllegalStateException("Inner.")),
                    "query-1", "node-b", null
            );

            // then
            assertThat(event.errorDetails()).contains("Outer.", "Inner.");
        }

        @Test
        void readsAFailureFromAMemberReportingACodeThisOneDoesNotKnow() {
            // given a member running a newer version, whose error code did not survive deserialization
            QueryDispatchFailure unknown = new QueryDispatchFailure(
                    "query-1", null, "Something went wrong.", java.util.List.of("Something went wrong."),
                    "node-b", null, null
            );

            // when
            RuntimeException rebuilt = QueryConverter.convertError(unknown);

            // then the query fails with what the member did say, rather than on the deserialization
            assertThat(rebuilt).isInstanceOf(QueryExecutionException.class);
            assertThat(rebuilt).hasMessageContaining("Something went wrong.");
        }

        @Test
        void describesAFailureThatCarriesNoMessage() {
            // when
            QueryDispatchFailure event = QueryConverter.convertErrorResult(
                    new IllegalStateException(), "query-1", "node-b", null
            );

            // then the type stands in, so the member that asked is not told simply "null"
            assertThat(event.errorMessage()).isEqualTo(IllegalStateException.class.getName());
        }
    }
}
