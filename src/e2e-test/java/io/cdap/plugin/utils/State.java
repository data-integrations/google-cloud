/**
 * Autogenerated by Avro
 * <p>
 * DO NOT EDIT DIRECTLY
 */
package io.cdap.plugin.utils;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

/** A list of states in the United States of America. */
@org.apache.avro.specific.AvroGenerated
public class State extends org.apache.avro.specific.SpecificRecordBase
  implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser()
    .parse("{\"type\":\"record\",\"name\":\"State\",\"namespace\":\"utilities\",\"doc\":\"A list of states in the" +
             " United States of America.\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"The common " +
             "name of the state.\"},{\"name\":\"post_abbr\",\"type\":\"string\",\"doc\":\"The postal code " +
             "abbreviation of the state.\"}]}");
  private static final long serialVersionUID = -6098929419967278282L;
  private static final SpecificData MODEL$ = new SpecificData();
  private static final BinaryMessageEncoder<State> ENCODER =
    new BinaryMessageEncoder<>(MODEL$, SCHEMA);
  private static final BinaryMessageDecoder<State> DECODER =
    new BinaryMessageDecoder<>(MODEL$, SCHEMA);
  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<State>
    WRITER$ = (org.apache.avro.io.DatumWriter<State>) MODEL$.createDatumWriter(SCHEMA);
  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<State>
    READER$ = (org.apache.avro.io.DatumReader<State>) MODEL$.createDatumReader(SCHEMA);
  /** The common name of the state. */
  private CharSequence name;
  /** The postal code abbreviation of the state. */
  private CharSequence postabbr;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public State() {
  }

  /**
   * All-args constructor.
   * @param name The common name of the state.
   * @param postabbr The postal code abbreviation of the state.
   */
  public State(CharSequence name, CharSequence postabbr) {
    this.name = name;
    this.postabbr = postabbr;
  }

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA;
  }

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<State> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<State> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<State> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA, resolver);
  }

  /**
   * Deserializes a State from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a State instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static State fromByteBuffer(
    java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /**
   * Creates a new State RecordBuilder.
   * @return A new State RecordBuilder
   */
  public static State.Builder newBuilder() {
    return new State.Builder();
  }

  /**
   * Creates a new State RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new State RecordBuilder
   */
  public static State.Builder newBuilder(State.Builder other) {
    if (other == null) {
      return new State.Builder();
    } else {
      return new State.Builder(other);
    }
  }

  /**
   * Creates a new State RecordBuilder by copying an existing State instance.
   * @param other The existing instance to copy.
   * @return A new State RecordBuilder
   */
  public static State.Builder newBuilder(State other) {
    if (other == null) {
      return new State.Builder();
    } else {
      return new State.Builder(other);
    }
  }

  /**
   * Serializes this State to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  @Override
  public SpecificData getSpecificData() {
    return MODEL$;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return SCHEMA;
  }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public Object get(int field) {
    switch (field) {
      case 0:
        return name;
      case 1:
        return postabbr;
      default:
        throw new IndexOutOfBoundsException("Invalid index: " + field);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value = "unchecked")
  public void put(int field, Object value) {
    switch (field) {
      case 0:
        name = (CharSequence) value;
        break;
      case 1:
        postabbr = (CharSequence) value;
        break;
      default:
        throw new IndexOutOfBoundsException("Invalid index: " + field);
    }
  }

  /**
   * Gets the value of the 'name' field.
   * @return The common name of the state.
   */
  public CharSequence getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * The common name of the state.
   * @param value the value to set.
   */
  public void setName(CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'post_abbr' field.
   * @return The postal code abbreviation of the state.
   */
  public CharSequence getPostAbbr() {
    return postabbr;
  }

  /**
   * Sets the value of the 'post_abbr' field.
   * The postal code abbreviation of the state.
   * @param value the value to set.
   */
  public void setPostAbbr(CharSequence value) {
    this.postabbr = value;
  }

  @Override
  public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @Override
  public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override
  protected boolean hasCustomCoders() {
    return true;
  }

  @Override
  public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException {
    out.writeString(this.name);

    out.writeString(this.postabbr);

  }

  @Override
  public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.name = in.readString(this.name instanceof Utf8 ? (Utf8) this.name : null);

      this.postabbr = in.readString(this.postabbr instanceof Utf8 ? (Utf8) this.postabbr : null);

    } else {
      for (int i = 0; i < 2; i++) {
        switch (fieldOrder[i].pos()) {
          case 0:
            this.name = in.readString(this.name instanceof Utf8 ? (Utf8) this.name : null);
            break;

          case 1:
            this.postabbr = in.readString(this.postabbr instanceof Utf8 ? (Utf8) this.postabbr : null);
            break;

          default:
            throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }

  /**
   * RecordBuilder for State instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<State>
    implements org.apache.avro.data.RecordBuilder<State> {

    /** The common name of the state. */
    private CharSequence name;
    /** The postal code abbreviation of the state. */
    private CharSequence postabbr;

    /** Creates a new Builder. */
    private Builder() {
      super(SCHEMA, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(State.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.postabbr)) {
        this.postabbr = data().deepCopy(fields()[1].schema(), other.postabbr);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing State instance.
     * @param other The existing instance to copy.
     */
    private Builder(State other) {
      super(SCHEMA, MODEL$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.postabbr)) {
        this.postabbr = data().deepCopy(fields()[1].schema(), other.postabbr);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Gets the value of the 'name' field.
     * The common name of the state.
     * @return The value.
     */
    public CharSequence getName() {
      return name;
    }


    /**
     * Sets the value of the 'name' field.
     * The common name of the state.
     * @param value The value of 'name'.
     * @return This builder.
     */
    public State.Builder setName(CharSequence value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
     * Checks whether the 'name' field has been set.
     * The common name of the state.
     * @return True if the 'name' field has been set, false otherwise.
     */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }


    /**
     * Clears the value of the 'name' field.
     * The common name of the state.
     * @return This builder.
     */
    public State.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
     * Gets the value of the 'post_abbr' field.
     * The postal code abbreviation of the state.
     * @return The value.
     */
    public CharSequence getPostAbbr() {
      return postabbr;
    }


    /**
     * Sets the value of the 'post_abbr' field.
     * The postal code abbreviation of the state.
     * @param value The value of 'post_abbr'.
     * @return This builder.
     */
    public State.Builder setPostAbbr(CharSequence value) {
      validate(fields()[1], value);
      this.postabbr = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
     * Checks whether the 'post_abbr' field has been set.
     * The postal code abbreviation of the state.
     * @return True if the 'post_abbr' field has been set, false otherwise.
     */
    public boolean hasPostAbbr() {
      return fieldSetFlags()[1];
    }


    /**
     * Clears the value of the 'post_abbr' field.
     * The postal code abbreviation of the state.
     * @return This builder.
     */
    public State.Builder clearPostAbbr() {
      postabbr = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public State build() {
      try {
        State record = new State();
        record.name = fieldSetFlags()[0] ? this.name : (CharSequence) defaultValue(fields()[0]);
        record.postabbr = fieldSetFlags()[1] ? this.postabbr : (CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
