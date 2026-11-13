/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link SourcingEventMessageStream}.
 *
 * @author Jakob Hatzl
 */
class SourcingEventMessageStreamTest {

    private ResultStream<SourceEventsResponse> stream;
    private SourcingEventMessageStream testSubject;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        stream = mock(ResultStream.class);
        when(stream.getError()).thenReturn(Optional.empty());

        TaggedEventConverter converter =
                new TaggedEventConverter(new DelegatingEventConverter(new JacksonConverter()), EventTypeResolver.DEFAULT);
        testSubject = new SourcingEventMessageStream(stream, converter);
    }

    @Nested
    class Close {

        @Test
        void doesNotCancelAStreamTheServerAlreadyCompleted() {
            // given the server completed the stream and all responses were consumed
            when(stream.isClosed()).thenReturn(true);

            // when
            testSubject.close();

            // then cancelling is skipped, as it would record a client-side close error on a completed stream
            verify(stream, never()).close();
        }

        @Test
        void cancelsAStreamThatIsStillOpen() {
            // given
            when(stream.isClosed()).thenReturn(false);

            // when
            testSubject.close();

            // then
            verify(stream).close();
        }
    }
}
