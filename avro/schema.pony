use "json"
use "debug"
use "collections"

// A Schema is represented in JSON by one of:
// * A JSON string, naming a defined type.
// * A JSON object, of the form:
//    `{"type": "typeName" ...attributes...}`
//   where typeName is either a primitive or derived type name, as
//   defined below. Attributes not defined in this document are permitted
//   as metadata, but must not affect the format of serialized data.
// * A JSON array, representing a union of embedded types.

primitive Namespace
  fun namespace_from(fullname: String): String ? =>
    let dot_loc = fullname.rfind(".")
    fullname.substring(0, dot_loc)

  fun apply(type_name: String, type_namespace: String,
    enclosing_namespace: String = ""): (String, String)
  ? =>
    if type_name.count(".") > 0 then
      (type_name, namespace_from(type_name))
    elseif type_namespace.size() > 0 then
      (".".join([type_namespace, type_name]), type_namespace)
    elseif enclosing_namespace.size() > 0 then
      (".".join([enclosing_namespace, type_name]), enclosing_namespace)
    else
      (type_name, "")
    end

class NullType
  new ref create() => None
  fun ref encoder(): Encoder => NullEncoder
  fun ref decoder(): Decoder => NullDecoder

class BooleanType
  new ref create() => None
  fun ref encoder(): Encoder => BooleanEncoder
  fun ref decoder(): Decoder => BooleanDecoder

class IntType
  new ref create() => None
  fun ref encoder(): Encoder => IntEncoder
  fun ref decoder(): Decoder => IntDecoder

class LongType
  new ref create() => None
  fun ref encoder(): Encoder => LongEncoder
  fun ref decoder(): Decoder => LongDecoder

class FloatType
  new ref create() => None
  fun ref encoder(): Encoder => FloatEncoder
  fun ref decoder(): Decoder => FloatDecoder

class DoubleType
  new ref create() => None
  fun ref encoder(): Encoder => DoubleEncoder
  fun ref decoder(): Decoder => DoubleDecoder

class BytesType
  new ref create() => None
  fun ref encoder(): Encoder => BytesEncoder
  fun ref decoder(): Decoder => BytesDecoder

class StringType
  new ref create() => None
  fun ref encoder(): Encoder => StringEncoder
  fun ref decoder(): Decoder => StringDecoder

type PrimitiveType is (NullType | BooleanType | IntType | LongType |
                       FloatType | DoubleType | BytesType | StringType)

class RecordType
  let _types: Array[Type]
  new create(types: Array[Type]) =>
    _types = types
  fun ref encoder(): Encoder ? =>
    let encoders = Array[Encoder]
    for t in _types.values() do
      encoders.push(t.encoder())
    end
    RecordEncoder(consume encoders)
  fun ref decoder(): Decoder ? =>
    let decoders = Array[Decoder]
    for t in _types.values() do
      decoders.push(t.decoder())
    end
    RecordDecoder(consume decoders)

class EnumType
  let _symbols: Array[EnumSymbol val] val
  new create(symbol_names: Array[String]) =>
    let sz = symbol_names.size()
    let symbols = recover Array[EnumSymbol val](sz) end
    for (idx, symbol_name) in symbol_names.pairs() do
      symbols.push(EnumSymbol(symbol_name, idx))
    end
    _symbols = consume symbols
  fun ref encoder(): Encoder =>
    EnumEncoder(_symbols)
  fun ref decoder(): Decoder =>
    EnumDecoder(_symbols)

class UnionType
  let _types: Array[Type]
  new create(types: Array[Type]) =>
    _types = types
  fun ref encoder(): Encoder ? =>
    let encoders = Array[Encoder]
    for t in _types.values() do
      encoders.push(t.encoder())
    end
    UnionEncoder(consume encoders)
  fun ref decoder(): Decoder ? =>
    let decoders = Array[Decoder]
    for t in _types.values() do
      decoders.push(t.decoder())
    end
    UnionDecoder(consume decoders)

class ArrayType
  let _type: Type
  new create(type': Type) =>
    _type = type'
  fun ref encoder(): Encoder ? =>
    ArrayEncoder(_type.encoder())
  fun ref decoder(): Decoder ? =>
    ArrayDecoder(_type.decoder())

class MapType
  let _type: Type
  new create(type': Type) =>
    _type = type'
  fun ref encoder(): Encoder ? =>
    MapEncoder(_type.encoder())
  fun ref decoder(): Decoder ? =>
    MapDecoder(_type.decoder())

class FixedType
  let _size: USize
  new create(size': USize) =>
    _size = size'
  fun ref encoder(): Encoder =>
    FixedEncoder(_size)
  fun ref decoder(): Decoder =>
    FixedDecoder(_size)

class _BogusType
  new ref create() =>
    None
  fun ref encoder(): Encoder ? =>
    error
  fun ref decoder(): Decoder ? =>
    error

class ForwardDeclarationType
  var _type: (Type | _BogusType) = _BogusType
  var _encoder_table: AvroEncoderSymbolTable
  var _decoder_table: AvroDecoderSymbolTable
  var _type_name: String = ""
  new create(encoder_table: AvroEncoderSymbolTable,
             decoder_table: AvroDecoderSymbolTable) =>
    _encoder_table = encoder_table
    _decoder_table = decoder_table
    
  fun ref set_body(type_name: String, type': Type) =>
    _type = type'
    _type_name = type_name
  fun ref encoder(): Encoder =>
    _encoder_table(_type_name)
  fun ref decoder(): Decoder =>
    _decoder_table(_type_name)

type ComplexType is (RecordType | EnumType | ArrayType | MapType | FixedType |
                     UnionType)

type Type is (PrimitiveType | ComplexType | ForwardDeclarationType)

class AvroTypeSymbolTable
  let _map: Map[String, Type] = Map[String, Type]
  var _encoder_symbol_table: AvroEncoderSymbolTable = AvroEncoderSymbolTable
  var _decoder_symbol_table: AvroDecoderSymbolTable = AvroDecoderSymbolTable
  fun ref apply(symbol: String): Type =>
    try
      if not _map.contains(symbol) then
        _map(symbol) = ForwardDeclarationType(_encoder_symbol_table,
                                              _decoder_symbol_table)
      end
      _map(symbol)
    else
      // never gets here
      ForwardDeclarationType(_encoder_symbol_table,
                             _decoder_symbol_table)
    end
  fun ref set_body(type_name: String, type': Type) ? =>
    try
      (this(type_name) as ForwardDeclarationType).set_body(type_name, type')
    else
      Debug("failed to set body for '" + type_name + "' in symbol table")
      error
    end
    try
      _encoder_symbol_table.set_body(type_name, type'.encoder())
    else
      Debug("failed to set encoder for '" + type_name + "' in symbol table")
      error
    end
    try
      _decoder_symbol_table.set_body(type_name, type'.decoder())
    else
      Debug("failed to set decoder for '" + type_name + "' in symbol table")
      error
    end

class AvroEncoderSymbolTable
  let _map: Map[String, Encoder] = Map[String, Encoder] 
  fun ref apply(symbol: String): Encoder =>
    try
      if _map.contains(symbol) then
        _map(symbol)
      else
        let fwd = ForwardDeclarationEncoder
        _map(symbol) = fwd
        fwd
      end
    else
      // never gets here
      ForwardDeclarationEncoder
    end
  fun ref set_body(type_name: String, encoder: Encoder) ? =>
    match this(type_name)
    | let forward_encoder: ForwardDeclarationEncoder =>
      forward_encoder.set_body(encoder)
    else
      Debug("failed to set encoder body for " + type_name)
      error
    end

class AvroDecoderSymbolTable
  let _map: Map[String, Decoder] = Map[String, Decoder]
  fun ref apply(symbol: String): Decoder =>
    try
      if _map.contains(symbol) then
        _map(symbol)
      else
        let fwd = ForwardDeclarationDecoder
        _map(symbol) = fwd
        fwd
      end
    else
      // never gets here
      ForwardDeclarationDecoder
    end
  fun ref set_body(type_name: String, decoder: Decoder) ? =>
    match this(type_name)
    | let forward_decoder: ForwardDeclarationDecoder =>
      forward_decoder.set_body(decoder)
    else
      Debug("failed to set decoder body for " + type_name)
      error
    end

class Schema
  let _json_doc: JsonDoc
  var _type: (Type | None) = None
  var _type_symbol_table: AvroTypeSymbolTable = AvroTypeSymbolTable
  new create(string: String) ? =>
    _json_doc = JsonDoc
    _json_doc.parse(string)
  fun ref encoder(): Encoder ? =>
    _get_type().encoder()

  fun ref decoder(): Decoder ? =>
    _get_type().decoder()

  fun ref _get_type(json_type: JsonType = None, namespace: String = ""):
    Type
  ? =>
    if (json_type is None) then
      if (_type is None) then
        _type = _get_type(_json_doc.data)
      end
      _type as Type
    else
      match json_type
      | let type_name: String val =>
        _type_name_to_type(type_name, namespace)
      | let complex_type: JsonObject =>
        let type_name = try
          (complex_type.data("type") as String)
        else
          Debug("failed to get name from complex type")
          error
        end
        match type_name
        | "record" =>
          _get_record_type(complex_type, namespace)
        | "enum" =>
          _get_enum_type(complex_type, namespace)
        | "array" =>
          _get_array_type(complex_type, namespace)
        | "map" =>
          _get_map_type(complex_type, namespace)
        | "fixed" =>
          _get_fixed_type(complex_type, namespace)
        else
          Debug("failed to get complex type's type, type='" + type_name +
            "' namespace='" + namespace + "'")
          error
        end
      | let union: JsonArray =>
        _get_union_type(union, namespace)
      else
        Debug("failed to get type from json type")
        error
      end
    end

  fun ref _type_name_to_type(type_name: String, namespace: String): Type ? =>
    match type_name
    | "null" => recover NullType end
    | "boolean" => recover BooleanType end
    | "int" => recover IntType end
    | "long" => recover LongType end
    | "float" => recover FloatType end
    | "double" => recover DoubleType end
    | "bytes" => recover BytesType end
    | "string" => recover StringType end
    else
      (let full_type_name, _) = Namespace(type_name, "", namespace)
      _type_symbol_table(full_type_name)
    end

  fun _fullname_namespace_aliases(named_obj: JsonObject,
    enclosing_namespace: String): (String, String, Array[String]) ?
  =>
    let type_name = named_obj.data("name") as String
    let aliases: JsonArray = try
      named_obj.data("aliases") as JsonArray
    else
      JsonArray
    end
    let type_namespace = try named_obj.data("namespace") as String else "" end
    (let fullname, let namespace) =
      Namespace(type_name, type_namespace, enclosing_namespace)
    let fullname_aliases = Array[String](aliases.data.size())
    for alias in aliases.data.values() do
      (let fullname_alias, _) = Namespace(alias as String, namespace)
      fullname_aliases.push(consume fullname_alias)
    end
    (fullname, namespace, consume fullname_aliases)

  fun ref _get_record_type(record: JsonObject, enclosing_namespace: String):
    Type ?
  =>
    let fields = record.data("fields") as JsonArray
    let sz = fields.data.size()
    let types = Array[Type](sz)
    (let fullname, let namespace, let aliases) =
      _fullname_namespace_aliases(record, enclosing_namespace)
    for f in fields.data.values() do
      let t = (f as JsonObject).data("type")
      types.push(_get_type(t, namespace))
    end
    let record_type = RecordType(consume types)
    _type_symbol_table.set_body(fullname, record_type)
    consume record_type

  fun ref _get_enum_type(enum: JsonObject, enclosing_namespace: String):
    Type ?
  =>
    let raw_symbol_names = enum.data("symbols") as JsonArray
    let sz = raw_symbol_names.data.size()
    (let fullname, let namespace, let aliases) =
      _fullname_namespace_aliases(enum, enclosing_namespace)
    // let name = enum.data("name") as String
    let symbol_names = recover Array[String val](sz) end
    for sn in raw_symbol_names.data.values() do
      symbol_names.push(sn as String)
    end
    let enum_type = EnumType(consume symbol_names)
    _type_symbol_table.set_body(fullname, enum_type)
    consume enum_type

  fun ref _get_array_type(array: JsonObject, enclosing_namespace: String):
    Type ?
  =>
    let items_type = array.data("items") as String
    ArrayType(_get_type(items_type, enclosing_namespace))

  fun ref _get_map_type(array: JsonObject, enclosing_namespace: String):
    Type ?
  =>
    let values_type = array.data("values") as String
    MapType(_get_type(values_type, enclosing_namespace))

  fun ref _get_fixed_type(fixed: JsonObject, enclosing_namespace: String):
    Type ?
  =>
    let size' = fixed.data("size") as I64
    (let fullname, let namespace, let aliases) =
      _fullname_namespace_aliases(fixed, enclosing_namespace)
    // let name = fixed.data("name") as String
    let fixed_type = FixedType(size'.usize())
    _type_symbol_table.set_body(fullname, fixed_type)
    consume fixed_type

  fun ref _get_union_type(union: JsonArray, enclosing_namespace: String):
    Type ?
  =>
    let sz = union.data.size()
    let types = Array[Type].reserve(sz)
    for t in union.data.values() do
      types.push(_get_type(t, enclosing_namespace))
    end
    UnionType(consume types)