use "json"
use "debug"

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

// type PrimitiveType is (NullType | BooleanType | IntType | LongType |
//                        FloatType | DoubleType | BytesType | StringType)

type PrimitiveType is (NullType val | StringType val | IntType val)

class RecordType
  let _types: Array[Type val] val
  new val create(types: Array[Type val] val) =>
    _types = types
  fun encoder(): Encoder =>
    let encoders = Array[Encoder]
    for t in _types.values() do
      encoders.push(t.encoder())
    end
    RecordEncoder(consume encoders)
  fun decoder(): Decoder =>
    let decoders = Array[Decoder]
    for t in _types.values() do
      decoders.push(t.decoder())
    end
    RecordDecoder(consume decoders)

class UnionType
  let _types: Array[Type val] val
  new val create(types: Array[Type val] val) =>
    _types = types
  fun encoder(): Encoder =>
    let encoders = Array[Encoder]
    for t in _types.values() do
      encoders.push(t.encoder())
    end
    UnionEncoder(consume encoders)
  fun decoder(): Decoder =>
    let decoders = Array[Decoder]
    for t in _types.values() do
      decoders.push(t.decoder())
    end
    UnionDecoder(consume decoders)

type Type is (PrimitiveType val | RecordType val | UnionType val)

class Schema
  let _json_doc: JsonDoc
  var _type: (Type | None) = None
  new create(string: String) ? =>
    _json_doc = JsonDoc
    _json_doc.parse(string)

  fun ref encoder(): Encoder ? =>
    Debug("getting encoder")
    _get_type().encoder()

  fun ref decoder(): Decoder ? =>
    _get_type().decoder()

  fun ref _get_type(json_type: JsonType = None): Type val ? =>
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
        error
      end
    end

  fun ref _type_name_to_type(type_name: String): Type val ? =>
    match type_name
    | "null" => recover NullType end
    | "string" => recover StringType end
    | "int" => recover IntType end
    else
      error
    end

  fun ref _get_record_type(record: JsonObject): Type val ? =>
    let sz = record.data.size()
    let types = recover Array[Type].reserve(sz) end
    let name = record.data("name") as String
    let fields = record.data("fields") as JsonArray
    for f in fields.data.values() do
      let t = (f as JsonObject).data("type")
      types.push(_get_type(t))
    end
    RecordType(consume types)

  fun ref _get_union_type(union: JsonArray): Type val ? =>
    let sz = union.data.size()
    let types = recover Array[Type].reserve(sz) end
    for t in union.data.values() do
      types.push(_get_type(t))
    end
    UnionType(consume types)