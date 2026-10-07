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

package org.axonframework.conversion.xstream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Metadata;
import org.jspecify.annotations.Nullable;

import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A {@link Converter} implementation that uses an application-supplied {@link XStream} instance to convert objects into
 * and from the XML format that Axon Framework 4's {@code XStreamSerializer} produced.
 * <p>
 * This {@code Converter} exists for exactly one purpose: giving an Axon Framework 5 application time to let deadlines
 * and events that an Axon Framework 4 node scheduled, and sagas it started, run their course, instead of forcing every
 * one of them to fire or complete before the switch to Axon Framework 5. Scheduled deadlines and events only need to be
 * read back in this format: once one fires, there is nothing left to write. A saga is different, since it keeps
 * handling events until it ends, so its state is both read and written back in XStream for as long as it stays active.
 * In practice that is Axon Framework 4 saga state (the legacy saga stores default to {@code XStreamSerializer} when
 * built without one), deadlines scheduled through a {@code DeadlineManager}, and events scheduled through an
 * {@code EventScheduler}.
 * <p>
 * Note this {@code Converter} implementation  is <b>not</b> meant to be used for anything new. It is never registered
 * by default, and it should be removed from an application's configuration as soon as all data it was reading has been
 * drained or rewritten in the application's regular {@link Converter} format.
 * <p>
 * Construction allows {@code "org.axonframework.**"} on the given {@link XStream} instance, the same baseline Axon
 * Framework 4's {@code XStreamSerializer} added by default. XStream deserializes by instantiating arbitrary classes
 * named in the XML it reads, which is unsafe against untrusted input unless the set of types it may instantiate is
 * restricted. This baseline allows every class in every Axon Framework jar on the classpath. Callers remain responsible
 * for allowing their <b>own</b> saga, deadline, and event payload classes themselves, typically with
 * {@link XStream#allowTypesByWildcard(String[])} or {@link XStream#allowTypes(Class[])}, before handing the
 * {@code XStream} instance to this constructor.
 * <p>
 * This {@code Converter} only ever converts a single payload, metadata map, or scope descriptor at a time; it never
 * sees a whole message envelope. Construction aliases {@link Metadata} to the same {@code <meta-data>} element Axon
 * Framework 4's {@code MetaData} used, and marks {@link UUID} as an immutable type aliased to {@code uuid}, so that XML
 * produced by an Axon Framework 4 {@code XStreamSerializer} round-trips through this {@code Converter} unchanged.
 * Ported scope descriptors such as {@code AggregateScopeDescriptor} and {@code SagaScopeDescriptor} keep their Axon
 * Framework 4 package and class name in this module, so no further aliasing is required for those to resolve against
 * Axon Framework 4-written XML.
 * <p>
 * A {@code byte[]}, {@link String}, or {@link InputStream} value is treated as already-serialized content on either
 * side of a conversion, unlike Axon Framework 4's {@code XStreamSerializer}, which always ran a payload through XStream
 * regardless of its Java type. Converting between two of these carrier types is therefore a plain content type
 * conversion around the XML, not a new XStream marshalling pass.
 * <p>
 * Running on Java 9 or later with reflective access restricted to named modules, XStream may need {@code --add-opens}
 * JVM arguments to reflect on types in {@code java.base} or other platform modules; see
 * <a href="https://x-stream.github.io/faq.html#Compatibility_cannot_access_from_unnamed_module">XStream's FAQ</a>.
 * <p>
 * Construct this {@code Converter} with a pre-configured {@link XStream} instance, for example the one an application
 * already uses with Axon Framework 4 (Spring Boot applications typically expose this as the {@code defaultAxonXStream}
 * bean), and pass it to the {@code converter(...)} builder method of a legacy saga store or deadline manager:
 * <pre>{@code
 * XStream xStream = new XStream();
 * xStream.allowTypesByWildcard(new String[]{"com.example.myapp.**"});
 * Converter xStreamConverter = new XStreamConverter(xStream);
 *
 * JpaSagaStore sagaStore = JpaSagaStore.builder()
 *                                      .entityManagerProvider(entityManagerProvider)
 *                                      .converter(xStreamConverter)
 *                                      .build();
 * }</pre>
 *
 * @author Steven van Beelen
 * @since 5.4.0
 * @deprecated Only intended to drain Axon Framework 4 XStream-serialized sagas, deadlines, and scheduled events during
 * a migration period. Remove it from an application's configuration once that data no longer exists.
 */
@Deprecated(forRemoval = true, since = "5.4.0")
public class XStreamConverter implements Converter {

    private final XStream xStream;
    private final ChainingContentTypeConverter converter;

    /**
     * Constructs an {@code XStreamConverter} using the given {@code xStream} instance.
     * <p>
     * This constructor automatically allows {@code "org.axonframework.**"} on it, so this module's ported types
     * resolve. The caller remains responsible for allowing their own application types through the given
     * {@code xStream} instance.
     *
     * @param xStream the {@link XStream} instance used to convert objects into and from XML
     */
    public XStreamConverter(XStream xStream) {
        this(xStream, new ChainingContentTypeConverter());
    }

    /**
     * Constructs an {@code XStreamConverter} using the given {@code xStream} instance and {@code converter}.
     * <p>
     * This constructor automatically allows {@code "org.axonframework.**"} on it, so this module's ported types
     * resolve. The caller remains responsible for allowing their own application types through the given
     * {@code xStream} instance.
     * <p>
     * This constructor should only be used when a specific {@link ClassLoader} should be given to the
     * {@link ChainingContentTypeConverter}, to ensure it loads the right set of
     * {@code ContentTypeConverter ContentTypeConverters}.
     *
     * @param xStream   the {@link XStream} instance used to convert objects into and from XML
     * @param converter the converter used for the {@code byte[]}/{@code String}/{@link InputStream} content type
     *                  conversions surrounding the XML (de)serialization
     */
    @Internal
    public XStreamConverter(XStream xStream, ChainingContentTypeConverter converter) {
        this.xStream = Objects.requireNonNull(xStream, "The XStream instance may not be null.");
        this.converter = Objects.requireNonNull(converter, "The ChainingContentTypeConverter may not be null.");

        xStream.allowTypesByWildcard(new String[]{"org.axonframework.**"});
        xStream.alias("meta-data", Metadata.class);
        xStream.registerConverter(new MetadataXStreamConverter(xStream.getMapper()));
        xStream.addImmutableType(UUID.class, true);
        xStream.alias("uuid", UUID.class);
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public <T> T convert(@Nullable Object input, Type targetType) {
        if (input == null) {
            return null;
        }

        Class<?> sourceType = input.getClass();
        if (sourceType.equals(targetType)) {
            return (T) input;
        }
        Class<?> targetClass = rawClassOf(targetType);

        try {
            if (converter.canConvert(sourceType, targetClass)) {
                // Neither side needs XStream; this is a plain content type conversion around the XML.
                return (T) converter.convert(input, targetClass);
            } else if (converter.canConvert(String.class, targetClass)) {
                // Writing: object -> XML -> requested representation.
                String xml = xStream.toXML(input);
                return (T) converter.convert(xml, targetClass);
            } else if (converter.canConvert(sourceType, String.class)) {
                // Reading: stored representation -> XML -> object.
                String xml = converter.convert(input, String.class);
                return (T) convertFromXml(xml, targetClass);
            } else {
                throw new ConversionException(
                        "XStreamConverter cannot convert from [" + sourceType.getName() + "] to ["
                                + targetClass.getName() + "]. One of the two must be a byte[], String, or "
                                + "InputStream, as XStreamConverter only converts to and from XML."
                );
            }
        } catch (XStreamException e) {
            throw new ConversionException(
                    "Exception while trying to convert object of type [" + sourceType.getName() + "] to ["
                            + targetClass.getName() + "] through XStream.", e
            );
        }
    }

    private static Class<?> rawClassOf(Type targetType) {
        if (targetType instanceof Class<?> targetClass) {
            return targetClass;
        }
        if (targetType instanceof ParameterizedType parameterizedType
                && parameterizedType.getRawType() instanceof Class<?> rawType) {
            return rawType;
        }
        throw new ConversionException(
                "The targetType [" + targetType + "] is not a Class or a ParameterizedType, while XStreamConverter "
                        + "can only resolve a raw Class to convert to."
        );
    }

    private Object convertFromXml(String xml, Class<?> targetClass) {
        Object result = xStream.fromXML(xml);
        if (result != null && !targetClass.isInstance(result)) {
            throw new ConversionException(
                    "XStream read an object of type [" + result.getClass().getName() + "], which is not a ["
                            + targetClass.getName() + "]."
            );
        }
        return result;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("xStream", xStream);
    }

    /**
     * Marshals and unmarshals {@link Metadata} the way Axon Framework 4's {@code MetaDataConverter} did for
     * {@code MetaData}: as a plain map, omitted entirely when empty, so the resulting {@code <meta-data>} element is
     * compatible with what an Axon Framework 4 {@code XStreamSerializer} reads and writes.
     */
    private static final class MetadataXStreamConverter extends MapConverter {

        private MetadataXStreamConverter(Mapper mapper) {
            super(mapper);
        }

        @Override
        public boolean canConvert(Class type) {
            return Metadata.class.equals(type);
        }

        @Override
        public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
            Metadata metadata = (Metadata) source;
            if (!metadata.isEmpty()) {
                super.marshal(new HashMap<>(metadata), writer, context);
            }
        }

        @Override
        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
            if (!reader.hasMoreChildren()) {
                return Metadata.emptyInstance();
            }
            Map<String, String> contents = new HashMap<>();
            //noinspection unchecked
            populateMap(reader, context, contents);
            return contents.isEmpty() ? Metadata.emptyInstance() : Metadata.from(contents);
        }
    }
}
