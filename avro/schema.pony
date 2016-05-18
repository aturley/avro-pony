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
    Debug("record encoder")
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

class UnionType
  let _types: Array[Type]
  new create(types: Array[Type]) =>
    _types = types
  fun ref encoder(): Encoder ? =>
    Debug("union encoder")
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

class _BogusType
  new ref create() =>
    None
  fun ref encoder(): Encoder ? =>
    Debug("tried to get encoder from _BogusType")
    error
  fun ref decoder(): Decoder ? =>
    Debug("tried to get decoder from _BogusType")
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
    Debug(_type_name + " encoder")
    _encoder_table(_type_name)
  fun ref decoder(): Decoder =>
    _decoder_table(_type_name)

type Type is (PrimitiveType | RecordType | UnionType |
              ForwardDeclarationType)

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
      Debug("set encoder body for " + type_name)
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

  fun ref _get_type(json_type: JsonType = None): Type ? =>
    if (json_type is None) then
      if (_type is None) then
        _type = _get_type(_json_doc.data)
      end
      _type as Type
    else
      match json_type
      | let type_name: String val =>
        _type_name_to_type(type_name)
      | let record: JsonObject =>
        _get_record_type(record)
      | let union: JsonArray =>
        _get_union_type(union)
      else
        Debug("failed to get type from json type")
        error
      end
    end

  fun ref _type_name_to_type(type_name: String): Type =>
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
      Debug("looking up '" + type_name + "' in the symbol table")
      _type_symbol_table(type_name)
    end

  fun ref _get_record_type(record: JsonObject): Type ? =>
    let sz = record.data.size()
    let types = Array[Type].reserve(sz)
    let name = record.data("name") as String
    let fields = record.data("fields") as JsonArray
    for f in fields.data.values() do
      let t = (f as JsonObject).data("type")
      types.push(_get_type(t))
    end
    let record_type = RecordType(consume types)
    _type_symbol_table.set_body(name, record_type)
    consume record_type

  fun ref _get_union_type(union: JsonArray): Type ? =>
    let sz = union.data.size()
    let types = Array[Type].reserve(sz)
    for t in union.data.values() do
      types.push(_get_type(t))
    end
    UnionType(consume types)