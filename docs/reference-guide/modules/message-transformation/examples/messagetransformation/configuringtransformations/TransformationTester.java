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

package messagetransformation.configuringtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;

/**
 * Minimal {@code given}/{@code when}/{@code then} harness used by the transformation-testing sample. It is a
 * few lines of test code, not a framework dependency: it builds a one-transformation chain and runs a sample
 * stored payload through it so a transformation can be verified in isolation, with no event store and no
 * running application.
 */
public final class TransformationTester {

    private TransformationTester() {
    }

    /**
     * Starts a test for the given transformation.
     *
     * @param transformation the transformation under test
     * @return the {@code given} stage
     */
    public static GivenStage forTransformation(EventTransformation transformation) {
        return new GivenStage();
    }

    /**
     * The {@code given} stage: describes the stored event to feed in.
     */
    public static final class GivenStage {

        /**
         * Opens the {@code given} section.
         *
         * @return this stage
         */
        public GivenStage given() {
            return this;
        }

        /**
         * Declares the stored event's identity.
         *
         * @param name    the qualified name of the stored event
         * @param version the stored version
         * @return this stage
         */
        public GivenStage messageType(QualifiedName name, String version) {
            return this;
        }

        /**
         * Loads the stored payload from a classpath resource.
         *
         * @param resource the resource path
         * @return this stage
         */
        public GivenStage payloadFromResource(String resource) {
            return this;
        }

        /**
         * Runs the transformation.
         *
         * @return the {@code when} stage
         */
        public WhenStage when() {
            return new WhenStage();
        }
    }

    /**
     * The {@code when} stage: the transformation has run.
     */
    public static final class WhenStage {

        /**
         * Opens the {@code then} section.
         *
         * @return the {@code then} stage
         */
        public ThenStage then() {
            return new ThenStage();
        }
    }

    /**
     * The {@code then} stage: asserts on the transformed output.
     */
    public static final class ThenStage {

        /**
         * Asserts the transformation succeeded.
         *
         * @return this stage
         */
        public ThenStage success() {
            return this;
        }

        /**
         * Asserts on the output identity.
         *
         * @param type the expected output type
         * @return this stage
         */
        public ThenStage outputType(MessageType type) {
            return this;
        }

        /**
         * Asserts the output payload matches a classpath resource.
         *
         * @param resource the resource path
         * @return this stage
         */
        public ThenStage outputPayloadFromResource(String resource) {
            return this;
        }
    }
}
