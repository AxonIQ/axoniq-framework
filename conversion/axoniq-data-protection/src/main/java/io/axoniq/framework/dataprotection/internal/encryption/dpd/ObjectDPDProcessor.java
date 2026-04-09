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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.framework.dataprotection.internal.encryption.dpd;

import io.axoniq.framework.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.internal.encryption.core.ByteArrayEncrypter;
import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.dataprotection.internal.encryption.ipd.RoutingIPDEncrypter;
import io.axoniq.framework.dataprotection.internal.encryption.spd.ConvertingEncrypter;
import io.axoniq.framework.dataprotection.internal.encryption.spd.ValuePair;
import io.axoniq.framework.dataprotection.internal.model.DPDField;
import io.axoniq.framework.dataprotection.internal.model.DataSubjIdField;
import io.axoniq.framework.dataprotection.internal.model.FieldEncryptionModel;
import io.axoniq.framework.dataprotection.internal.model.IPDField;
import io.axoniq.framework.dataprotection.internal.model.MPDField;
import io.axoniq.framework.dataprotection.internal.model.MapKeyField;
import io.axoniq.framework.dataprotection.internal.model.MapValueField;
import io.axoniq.framework.dataprotection.internal.model.ModelRegistry;
import io.axoniq.framework.dataprotection.internal.model.SPDField;
import io.axoniq.framework.dataprotection.internal.utils.CollectionUtils;
import io.axoniq.framework.dataprotection.internal.utils.ExceptionFactory;
import io.axoniq.framework.dataprotection.internal.utils.MutableObjectWrapper;
import org.axonframework.conversion.Converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;

import static io.axoniq.framework.dataprotection.internal.utils.ScalaDetector.isScalaPresent;

/**
 * Implementation of {@link DPDProcessor} for regular objects. Processes all personal data fields
 * of an object, including IPD, SPD, MPD, and nested DPD fields.
 *
 * @author Frans van Buul
 */
class ObjectDPDProcessor implements DPDProcessor<Object> {

    private final ModelRegistry modelRegistry;
    private final CryptoEngine cryptoEngine;
    private final RoutingDPDProcessor routingDPDProcessor;
    private final RoutingIPDEncrypter routingIPDEncrypter;
    private final ConvertingEncrypter convertingEncrypter;

    /**
     * Creates an ObjectDPDProcessor using the Converter API (Axon Framework 5.x).
     * <p>
     * This is the recommended constructor for Axon Framework 5.x applications.
     *
     * @param modelRegistry the registry for field encryption models
     * @param cryptoEngine the crypto engine for key management
     * @param routingDPDProcessor the routing processor for deep personal data
     * @param converter the converter for object serialization
     * @param replacementValueProvider the provider for replacement values
     * @since 5.0.0
     */
    public ObjectDPDProcessor(ModelRegistry modelRegistry, CryptoEngine cryptoEngine, RoutingDPDProcessor routingDPDProcessor, Converter converter, ReplacementValueProvider replacementValueProvider) {
        this.modelRegistry = modelRegistry;
        this.cryptoEngine = cryptoEngine;
        this.routingDPDProcessor = routingDPDProcessor;
        this.routingIPDEncrypter = new RoutingIPDEncrypter(cryptoEngine, replacementValueProvider);
        this.convertingEncrypter = new ConvertingEncrypter(new ByteArrayEncrypter(cryptoEngine, replacementValueProvider), converter, replacementValueProvider);
    }

    @Override
    public Object process(Object input, EncryptionContext context, Operation operation) {
        FieldEncryptionModel model = modelRegistry.get(input.getClass());

        // Wrap the object to handle both mutable and immutable (record) objects
        MutableObjectWrapper wrapper =
            new MutableObjectWrapper(input);

        pushKeys(model, wrapper, context, operation);
        if(operation == Operation.ENCRYPT || operation == Operation.REPLACE) {
            processSPD(model, wrapper, context, operation);
        }
        processIPD(model, wrapper, context, operation);
        processDPD(model, wrapper, context, operation);
        processMPD(model, wrapper, context, operation);
        if(operation == Operation.DECRYPT) {
            processSPD(model, wrapper, context, operation);
        }
        popKeys(model, context);

        // Get the result - for records this creates a new instance with all modifications
        try {
            return wrapper.getResult();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create modified record instance", e);
        }
    }

    private void pushKeys(FieldEncryptionModel model, MutableObjectWrapper wrapper, EncryptionContext context, Operation operation) {
        for(DataSubjIdField dataSubjIdField : model.getDataSubjIdFields().stream().filter(x -> context.mustProcess(x.getGroup())).collect(Collectors.toList())) {
            Object keyIdObj = wrapper.getFieldValue(dataSubjIdField.getField());
            if(keyIdObj == null) throw new NullPointerException("@DataSubjectId field is null");
            String keyId = dataSubjIdField.getPrefix() + keyIdObj.toString();
            SecretKey key;
            switch(operation) {
                case ENCRYPT: key = cryptoEngine.getOrCreateKey(keyId); break;
                case DECRYPT: key = cryptoEngine.getKey(keyId); break;
                case REPLACE: key = null; break;
                default: throw ExceptionFactory.missingOperation();
            }
            context.push(dataSubjIdField.getGroup(), key);
        }
    }

    private void popKeys(FieldEncryptionModel model, EncryptionContext context) {
        for(DataSubjIdField dataSubjIdField : model.getDataSubjIdFields().stream().filter(x -> context.mustProcess(x.getGroup())).collect(Collectors.toList())) {
            context.pop(1);
        }
    }

    private void processIPD(FieldEncryptionModel model, MutableObjectWrapper wrapper, EncryptionContext context, Operation operation) {
        List<IPDField> ipdFieldList = new ArrayList<>(model.getIpdFields());
        if(operation == Operation.DECRYPT) {
            Collections.reverse(ipdFieldList);
        }
        for(IPDField ipdField : ipdFieldList.stream().filter(x -> context.mustProcess(x.getGroup())).collect(Collectors.toList())) {
            Object inputFieldValue = wrapper.getFieldValue(ipdField.getField());
            Optional<SecretKey> key = context.findKey(ipdField.getGroup());
            Object outputFieldValue = routingIPDEncrypter.process(ipdField, inputFieldValue, key, operation);
            if(inputFieldValue != outputFieldValue) {
                wrapper.setFieldValue(ipdField.getField(), outputFieldValue);
            }
        }
    }

    private void processDPD(FieldEncryptionModel model, MutableObjectWrapper wrapper, EncryptionContext context, Operation operation) {
        for(DPDField dpdField : model.getDpdFields()) {
            Object inputFieldValue = wrapper.getFieldValue(dpdField.getField());
            Object outputFieldValue = routingDPDProcessor.process(inputFieldValue, context, operation);
            // For DPD fields, the nested object might be replaced (e.g., if it's a record)
            if(inputFieldValue != outputFieldValue) {
                wrapper.setFieldValue(dpdField.getField(), outputFieldValue);
            }
        }
    }

    private void processSPD(FieldEncryptionModel model, MutableObjectWrapper wrapper, EncryptionContext context, Operation operation) {
        for(SPDField spdField : model.getSpdFields().stream().filter(x -> context.mustProcess(x.getGroup())).collect(Collectors.toList())) {
            Object valueFieldValue = wrapper.getFieldValue(spdField.getField());
            Object storageFieldValue = wrapper.getFieldValue(spdField.getTarget());
            ValuePair pair = new ValuePair(valueFieldValue, storageFieldValue, spdField.getTarget().getType());
            Optional<SecretKey> key = context.findKey(spdField.getGroup());

            convertingEncrypter.process(spdField, pair, key, operation);

            if(valueFieldValue != pair.getValue()) {
                wrapper.setFieldValue(spdField.getField(), pair.getValue());
            }
            if(storageFieldValue != pair.getStorage()) {
                wrapper.setFieldValue(spdField.getTarget(), pair.getStorage());
            }
        }
    }

    private void processMPD(FieldEncryptionModel model, MutableObjectWrapper wrapper, EncryptionContext context, Operation operation) {
        for(MPDField mpdField : model.getMpdFields()) {
            Object inputMapObject = wrapper.getFieldValue(mpdField.getField());
            Map inputMap = null;
            if(inputMapObject instanceof Map) {
                inputMap = (Map)inputMapObject;
            } else if(isScalaPresent() && inputMapObject instanceof scala.collection.Map) {
                scala.collection.Map inputScalaMap = (scala.collection.Map)inputMapObject;
                inputMap = new LinkedHashMap();
                scala.collection.Iterator<scala.Tuple2> iterator = inputScalaMap.iterator();
                while(iterator.hasNext()) {
                    scala.Tuple2 tuple = iterator.next();
                    inputMap.put(tuple._1, tuple._2);
                }
            }
            if(inputMap != null) {
                Map outputMap = new LinkedHashMap();
                boolean changed = false;
                for (Object entry : inputMap.entrySet()) {
                    boolean entryChanged = processMapEntry(context, operation, mpdField, outputMap, (Map.Entry) entry);
                    changed = changed || entryChanged;
                }
                if(changed) {
                    if(inputMapObject instanceof Map) {
                        /* java case */
                        if(isModifyImmutableCollections()) {
                            CollectionUtils.replaceAll(inputMap, outputMap);
                        } else {
                            inputMap.clear();
                            inputMap.putAll(outputMap);
                        }
                    } else if(isScalaPresent() && inputMapObject instanceof scala.collection.Map) {
                        /* scala case */
                        if(inputMapObject instanceof scala.collection.mutable.Map) {
                            scala.collection.mutable.Map inputMutableMap = (scala.collection.mutable.Map)inputMapObject;
                            inputMutableMap.clear();
                            for(Object entryObj : outputMap.entrySet()) {
                                Map.Entry entry = (Map.Entry)entryObj;
                                inputMutableMap.put(entry.getKey(), entry.getValue());
                            }
                        } else {
                            scala.collection.Map inputScalaMapLike = (scala.collection.Map)inputMapObject;
                            scala.collection.mutable.Builder builder = inputScalaMapLike.newSpecificBuilder();
                            for(Object entryObj : outputMap.entrySet()) {
                                Map.Entry entry = (Map.Entry)entryObj;
                                builder.$plus$eq(new scala.Tuple2(entry.getKey(), entry.getValue()));
                            }
                            Object outputMapObject = builder.result();
                            if(mpdField.getField().getType().isInstance(outputMapObject)) {
                                wrapper.setFieldValue(mpdField.getField(), outputMapObject);
                            } else {
                                throw ExceptionFactory.immutableCollection(mpdField.getField());
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean processMapEntry(EncryptionContext context, Operation operation, MPDField mpdField, Map outputMap, Map.Entry entry) {
        boolean changed = false;
        Object inputKey = entry.getKey();
        Object inputValue = entry.getValue();
        Object outputKey = inputKey;
        Object outputValue = inputValue;
        for(DataSubjIdField dataSubjIdField : mpdField.getDataSubjIdFields().stream().filter(x -> context.mustProcess(x.getGroup())).collect(Collectors.toList())) {
            if (inputKey == null) throw new NullPointerException("@DataSubjectId field in map is null");
            String keyId = dataSubjIdField.getPrefix() + inputKey.toString();
            SecretKey key;
            switch (operation) {
                case ENCRYPT:
                    key = cryptoEngine.getOrCreateKey(keyId);
                    break;
                case DECRYPT:
                    key = cryptoEngine.getKey(keyId);
                    break;
                case REPLACE:
                    key = null;
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            context.push(dataSubjIdField.getGroup(), key);
        }
        List<MapKeyField> mapKeyFields = new ArrayList<>(mpdField.getKey());
        if(operation == Operation.DECRYPT) Collections.reverse(mapKeyFields);
        for(MapKeyField mapKeyField : mapKeyFields) {
            if (mapKeyField instanceof IPDField && context.mustProcess(((IPDField) mapKeyField).getGroup())) {
                Optional<SecretKey> key = context.findKey(((IPDField) mapKeyField).getGroup());
                outputKey = routingIPDEncrypter.process(((IPDField) mapKeyField), inputKey, key, operation);
                if(inputKey != outputKey) changed = true;
                inputKey = outputKey;
            } else if (mapKeyField instanceof DPDField) {
                routingDPDProcessor.process(outputKey, context, operation);
            }
        }
        List<MapValueField> mapValueFields = new ArrayList<>(mpdField.getValue());
        if(operation == Operation.DECRYPT) Collections.reverse(mapValueFields);
        for(MapValueField mapValueField : mapValueFields) {
            if (mapValueField instanceof IPDField && context.mustProcess(((IPDField) mapValueField).getGroup())) {
                Optional<SecretKey> key = context.findKey(((IPDField) mapValueField).getGroup());
                outputValue = routingIPDEncrypter.process(((IPDField) mapValueField), inputValue, key, operation);
                if(inputValue != outputValue) changed = true;
                inputValue = outputValue;
            } else if (mapValueField instanceof DPDField) {
                routingDPDProcessor.process(outputValue, context, operation);
            }
        }
        for(DataSubjIdField dataSubjIdField : mpdField.getDataSubjIdFields().stream().filter(x -> context.mustProcess(x.getGroup())).collect(Collectors.toList())) {
            context.pop(1);
        }
        outputMap.put(outputKey, outputValue);
        return changed;
    }

    public boolean isModifyImmutableCollections() {
        return routingIPDEncrypter.isModifyImmutableCollections();
    }

    public void setModifyImmutableCollections(boolean value) {
        routingIPDEncrypter.setModifyImmutableCollections(value);
    }
}
