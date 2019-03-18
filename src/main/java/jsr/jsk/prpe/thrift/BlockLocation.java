/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package jsr.jsk.prpe.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.11.0)", date = "2019-03-18")
public class BlockLocation implements org.apache.thrift.TBase<BlockLocation, BlockLocation._Fields>, java.io.Serializable, Cloneable, Comparable<BlockLocation> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("BlockLocation");

  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField BLOCKNUMBER_FIELD_DESC = new org.apache.thrift.protocol.TField("blocknumber", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField LOCATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("locations", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new BlockLocationStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new BlockLocationTupleSchemeFactory();

  public int type; // required
  public int blocknumber; // required
  public java.util.List<DataNodeLocation> locations; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TYPE((short)1, "type"),
    BLOCKNUMBER((short)2, "blocknumber"),
    LOCATIONS((short)3, "locations");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TYPE
          return TYPE;
        case 2: // BLOCKNUMBER
          return BLOCKNUMBER;
        case 3: // LOCATIONS
          return LOCATIONS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __TYPE_ISSET_ID = 0;
  private static final int __BLOCKNUMBER_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32        , "int32")));
    tmpMap.put(_Fields.BLOCKNUMBER, new org.apache.thrift.meta_data.FieldMetaData("blocknumber", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32        , "int32")));
    tmpMap.put(_Fields.LOCATIONS, new org.apache.thrift.meta_data.FieldMetaData("locations", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, DataNodeLocation.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(BlockLocation.class, metaDataMap);
  }

  public BlockLocation() {
  }

  public BlockLocation(
    int type,
    int blocknumber,
    java.util.List<DataNodeLocation> locations)
  {
    this();
    this.type = type;
    setTypeIsSet(true);
    this.blocknumber = blocknumber;
    setBlocknumberIsSet(true);
    this.locations = locations;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public BlockLocation(BlockLocation other) {
    __isset_bitfield = other.__isset_bitfield;
    this.type = other.type;
    this.blocknumber = other.blocknumber;
    if (other.isSetLocations()) {
      java.util.List<DataNodeLocation> __this__locations = new java.util.ArrayList<DataNodeLocation>(other.locations.size());
      for (DataNodeLocation other_element : other.locations) {
        __this__locations.add(new DataNodeLocation(other_element));
      }
      this.locations = __this__locations;
    }
  }

  public BlockLocation deepCopy() {
    return new BlockLocation(this);
  }

  @Override
  public void clear() {
    setTypeIsSet(false);
    this.type = 0;
    setBlocknumberIsSet(false);
    this.blocknumber = 0;
    this.locations = null;
  }

  public int getType() {
    return this.type;
  }

  public BlockLocation setType(int type) {
    this.type = type;
    setTypeIsSet(true);
    return this;
  }

  public void unsetType() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __TYPE_ISSET_ID);
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __TYPE_ISSET_ID);
  }

  public void setTypeIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __TYPE_ISSET_ID, value);
  }

  public int getBlocknumber() {
    return this.blocknumber;
  }

  public BlockLocation setBlocknumber(int blocknumber) {
    this.blocknumber = blocknumber;
    setBlocknumberIsSet(true);
    return this;
  }

  public void unsetBlocknumber() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __BLOCKNUMBER_ISSET_ID);
  }

  /** Returns true if field blocknumber is set (has been assigned a value) and false otherwise */
  public boolean isSetBlocknumber() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __BLOCKNUMBER_ISSET_ID);
  }

  public void setBlocknumberIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __BLOCKNUMBER_ISSET_ID, value);
  }

  public int getLocationsSize() {
    return (this.locations == null) ? 0 : this.locations.size();
  }

  public java.util.Iterator<DataNodeLocation> getLocationsIterator() {
    return (this.locations == null) ? null : this.locations.iterator();
  }

  public void addToLocations(DataNodeLocation elem) {
    if (this.locations == null) {
      this.locations = new java.util.ArrayList<DataNodeLocation>();
    }
    this.locations.add(elem);
  }

  public java.util.List<DataNodeLocation> getLocations() {
    return this.locations;
  }

  public BlockLocation setLocations(java.util.List<DataNodeLocation> locations) {
    this.locations = locations;
    return this;
  }

  public void unsetLocations() {
    this.locations = null;
  }

  /** Returns true if field locations is set (has been assigned a value) and false otherwise */
  public boolean isSetLocations() {
    return this.locations != null;
  }

  public void setLocationsIsSet(boolean value) {
    if (!value) {
      this.locations = null;
    }
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((java.lang.Integer)value);
      }
      break;

    case BLOCKNUMBER:
      if (value == null) {
        unsetBlocknumber();
      } else {
        setBlocknumber((java.lang.Integer)value);
      }
      break;

    case LOCATIONS:
      if (value == null) {
        unsetLocations();
      } else {
        setLocations((java.util.List<DataNodeLocation>)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TYPE:
      return getType();

    case BLOCKNUMBER:
      return getBlocknumber();

    case LOCATIONS:
      return getLocations();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TYPE:
      return isSetType();
    case BLOCKNUMBER:
      return isSetBlocknumber();
    case LOCATIONS:
      return isSetLocations();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof BlockLocation)
      return this.equals((BlockLocation)that);
    return false;
  }

  public boolean equals(BlockLocation that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_type = true;
    boolean that_present_type = true;
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (this.type != that.type)
        return false;
    }

    boolean this_present_blocknumber = true;
    boolean that_present_blocknumber = true;
    if (this_present_blocknumber || that_present_blocknumber) {
      if (!(this_present_blocknumber && that_present_blocknumber))
        return false;
      if (this.blocknumber != that.blocknumber)
        return false;
    }

    boolean this_present_locations = true && this.isSetLocations();
    boolean that_present_locations = true && that.isSetLocations();
    if (this_present_locations || that_present_locations) {
      if (!(this_present_locations && that_present_locations))
        return false;
      if (!this.locations.equals(that.locations))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + type;

    hashCode = hashCode * 8191 + blocknumber;

    hashCode = hashCode * 8191 + ((isSetLocations()) ? 131071 : 524287);
    if (isSetLocations())
      hashCode = hashCode * 8191 + locations.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(BlockLocation other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetType()).compareTo(other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetBlocknumber()).compareTo(other.isSetBlocknumber());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlocknumber()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blocknumber, other.blocknumber);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetLocations()).compareTo(other.isSetLocations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.locations, other.locations);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("BlockLocation(");
    boolean first = true;

    sb.append("type:");
    sb.append(this.type);
    first = false;
    if (!first) sb.append(", ");
    sb.append("blocknumber:");
    sb.append(this.blocknumber);
    first = false;
    if (!first) sb.append(", ");
    sb.append("locations:");
    if (this.locations == null) {
      sb.append("null");
    } else {
      sb.append(this.locations);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'type' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'blocknumber' because it's a primitive and you chose the non-beans generator.
    if (locations == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'locations' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class BlockLocationStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public BlockLocationStandardScheme getScheme() {
      return new BlockLocationStandardScheme();
    }
  }

  private static class BlockLocationStandardScheme extends org.apache.thrift.scheme.StandardScheme<BlockLocation> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, BlockLocation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = iprot.readI32();
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BLOCKNUMBER
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.blocknumber = iprot.readI32();
              struct.setBlocknumberIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LOCATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.locations = new java.util.ArrayList<DataNodeLocation>(_list8.size);
                DataNodeLocation _elem9;
                for (int _i10 = 0; _i10 < _list8.size; ++_i10)
                {
                  _elem9 = new DataNodeLocation();
                  _elem9.read(iprot);
                  struct.locations.add(_elem9);
                }
                iprot.readListEnd();
              }
              struct.setLocationsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetType()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'type' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetBlocknumber()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'blocknumber' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, BlockLocation struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(TYPE_FIELD_DESC);
      oprot.writeI32(struct.type);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(BLOCKNUMBER_FIELD_DESC);
      oprot.writeI32(struct.blocknumber);
      oprot.writeFieldEnd();
      if (struct.locations != null) {
        oprot.writeFieldBegin(LOCATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.locations.size()));
          for (DataNodeLocation _iter11 : struct.locations)
          {
            _iter11.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class BlockLocationTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public BlockLocationTupleScheme getScheme() {
      return new BlockLocationTupleScheme();
    }
  }

  private static class BlockLocationTupleScheme extends org.apache.thrift.scheme.TupleScheme<BlockLocation> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, BlockLocation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.type);
      oprot.writeI32(struct.blocknumber);
      {
        oprot.writeI32(struct.locations.size());
        for (DataNodeLocation _iter12 : struct.locations)
        {
          _iter12.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, BlockLocation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.type = iprot.readI32();
      struct.setTypeIsSet(true);
      struct.blocknumber = iprot.readI32();
      struct.setBlocknumberIsSet(true);
      {
        org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.locations = new java.util.ArrayList<DataNodeLocation>(_list13.size);
        DataNodeLocation _elem14;
        for (int _i15 = 0; _i15 < _list13.size; ++_i15)
        {
          _elem14 = new DataNodeLocation();
          _elem14.read(iprot);
          struct.locations.add(_elem14);
        }
      }
      struct.setLocationsIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

