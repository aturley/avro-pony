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

primitive NullType
  fun encoder(): Encoder => NullEncoder
  fun decoder(): Decoder => NullDecoder

primitive StringType
  fun encoder(): Encoder => StringEncoder
  fun decoder(): Decoder => StringDecoder

primitive IntType
  fun encoder(): Encoder => IntEncoder
  fun decoder(): Decoder => IntDecoder

primitive LongType
  fun encoder(): Encoder => LongEncoder
  fun decoder(): Decoder => LongDecoder

// type PrimitiveType is (NullType | BooleanType | IntType | LongType |
//                        FloatType | DoubleType | BytesType | StringType)

type PrimitiveType is (NullType val | StringType val | IntType val |
                       LongType val)

class RecordType
  let _types: Array[Type] val
  new val create(types: Array[Type] val) =>
    _types = types
  fun encoder(): Encoder ? =>
    let encoders = Array[Encoder]
    for t in _types.values() do
      encoders.push(t.encoder())
    end
    RecordEncoder(consume encoders)
  fun decoder(): Decoder ? =>
    let decoders = Array[Decoder]
    for t in _types.values() do
      decoders.push(t.decoder())
    end
    RecordDecoder(consume decoders)

class UnionType
  let _types: Array[Type] val
  new val create(types: Array[Type] val) =>
    _types = types
  fun encoder(): Encoder ? =>
    let encoders = Array[Encoder]
    for t in _types.values() do
      encoders.push(t.encoder())
    end
    UnionEncoder(consume encoders)
  fun decoder(): Decoder ? =>
    let decoders = Array[Decoder]
    for t in _types.values() do
      decoders.push(t.decoder())
    end
    UnionDecoder(consume decoders)

primitive BogusType
  fun encoder(): Encoder ? =>
    error
  fun decoder(): Decoder ? =>
    error

class ForwardDeclarationType
  var _type: (Type | BogusType) = BogusType
  new ref create() =>
    None
  fun ref set_body(type': Type) =>
    _type = type'
  fun encoder(): Encoder ? =>
    _type.encoder()
  fun decoder(): Decoder ? =>
    _type.decoder()

type Type is (PrimitiveType val | RecordType val | UnionType val |
              ForwardDeclarationType val)

// class AvroTypeSymbolTable
//   let _map: Map[String, ForwardDeclarationType iso] =
//     Map[String, ForwardDeclarationType]
//   fun ref apply(symbol: String): ForwardDeclarationType iso^ =>
//     try
//       if _map.contains(symbol) then
//         _map(symbol)
//       else
//         let fwd = recover ForwardDeclarationType end
//         _map(symbol) = fwd
//         consume fwd
//       end
//     else
//       // never gets here
//       recover ForwardDeclarationType end
//     end
//   fun ref set_body(type_name: String, type': Type) ? =>
//     _map(type_name).set_body(type')

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
    match _map(type_name)
    | let forward_encoder: ForwardDeclarationEncoder =>
      forward_encoder.set_body(encoder)
    else
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
    match _map(type_name)
    | let forward_decoder: ForwardDeclarationDecoder =>
      forward_decoder.set_body(decoder)
    else
      error
    end

class Schema
  let _json_doc: JsonDoc
  var _type: (Type | None) = None
  var _encoder_symbol_table: AvroEncoderSymbolTable = AvroEncoderSymbolTable
  var _decoder_symbol_table: AvroDecoderSymbolTable = AvroDecoderSymbolTable
  // var _type_symbol_table: AvroTypeSymbolTable = AvroTypeSymbolTable
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
        Debug("UNION!")
        _get_union_type(union)
      else
        error
      end
    end

  fun ref _type_name_to_type(type_name: String): Type ? =>
    Debug("looking for type_name=" + type_name)
    match type_name
    | "null" => recover NullType end
    | "string" => recover StringType end
    | "int" => recover IntType end
    | "long" => recover LongType end
    else
      // _type_symbol_table(type_name)
      error
    end

  fun ref _get_record_type(record: JsonObject): Type ? =>
    let sz = record.data.size()
    let types = recover Array[Type].reserve(sz) end
    let name = record.data("name") as String
    let fields = record.data("fields") as JsonArray
    for f in fields.data.values() do
      let t = (f as JsonObject).data("type")
      types.push(_get_type(t))
    end
    let record_type = RecordType(consume types)
    // _type_symbol_table(name).set_body(record_type)
    consume record_type

  fun ref _get_union_type(union: JsonArray): Type ? =>
    let sz = union.data.size()
    let types = recover Array[Type].reserve(sz) end
    for t in union.data.values() do
      match t
      | let xyz: String =>
        Debug("in union, looking for type_name=" + xyz)
      else
        None
      end
      types.push(_get_type(t))
    end
    UnionType(consume types)