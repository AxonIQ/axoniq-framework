package io.axoniq.workflow.runtime.payload;

public record TypedValue<T>(
  Class<T> type,
  Object value
) {

  public static TypedValue<Void> NULL = new TypedValue<>(Void.class, null);

  public T getTyped() {
    //noinspection unchecked
    return (T) value;
  }

  public <X> X getAsTyped() {
    //noinspection unchecked
    return (X) value;
  }

  @Override
  public String toString() {
    return value != null ? value.toString() : "";
  }



}
