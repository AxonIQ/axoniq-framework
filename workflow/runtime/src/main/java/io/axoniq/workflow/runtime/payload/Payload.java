package io.axoniq.workflow.runtime.payload;

import jakarta.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * TODO: do we need typed values or is Map<String, Object> sufficient? 
 */
public class Payload implements Map<String, TypedValue<?>> {

  private static final Payload EMPTY_PAYLOAD = new Payload();
  private final Map<String, TypedValue<?>> payload = new HashMap<>();


  public static Payload empty() {
    return EMPTY_PAYLOAD;
  }

  public static Payload of(Payload other) {
    var copy =  new Payload();
    copy.payload.putAll(other.payload);
    return copy;
  }

  public Payload plus(Payload payload) {
    putAll(payload.payload);
    return this;
  }

  @Override
  public int size() {
    return payload.size();
  }

  @Override
  public boolean isEmpty() {
    return payload.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return payload.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return payload.containsValue(value);
  }

  @Override
  public TypedValue<?> get(Object key) {
    return payload.getOrDefault(key, TypedValue.NULL);
  }

  @Override
  public TypedValue<?> put(String key, TypedValue<?> value) {
    return payload.put(key, value);
  }

  @Override
  public TypedValue<?> remove(Object key) {
    return payload.remove(key);
  }

  @Override
  public void putAll(@Nonnull Map<? extends String, ? extends TypedValue<?>> other) {
    payload.putAll(other);
  }

  @Override
  public void clear() {
    payload.clear();
  }

  @Override
  @Nonnull
  public Set<String> keySet() {
    return payload.keySet();
  }

  @Override
  @Nonnull
  public Collection<TypedValue<?>> values() {
    return payload.values();
  }

  @Override
  @Nonnull
  public Set<Entry<String, TypedValue<?>>> entrySet() {
    return payload.entrySet();
  }

  public <T> Payload withValue(String name, Class<T> type, T value) {
    put(name, new TypedValue<>(type, value));
    return this;
  }

  public <T> Payload withValue(String name, T value) {
    put(name, new TypedValue<>(value.getClass(), value));
    return this;
  }

  @Override
  public String toString() {
    if (payload.isEmpty()) {
      return "{}";
    }
    return "{ " + String.join(", ",
      payload.entrySet()
      .stream()
      .map(e -> e.getKey() + "=" + e.getValue().toString()).toList())
      + " }";
  }

}
