use "net"
use "collections"
use "buffered"

interface Encoder
  fun ref encode(obj: AvroType val, buffer: Writer) ?

class NullEncoder is Encoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) =>
    None

class BooleanEncoder is Encoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    buffer.u8(if (obj as Bool) then 1 else 0 end)

class _VarIntEncoder is Encoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let long = obj as I64

    var remaining = (if long >= 0 then long else (not long) end) << 1

    var zigzag = (remaining.u8() and 0x7F) or
                 ((long >> 63).u8() and 1)

    while remaining != 0 do
      buffer.u8(zigzag or (1 << 7))
      remaining = remaining >> 7
      zigzag = remaining.u8() and 0x7F
    end
    buffer.u8(zigzag)

class IntEncoder is Encoder
  let _var_int_encoder: _VarIntEncoder = _VarIntEncoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    _var_int_encoder.encode((obj as I32).i64(), buffer)

class LongEncoder is Encoder
  let _var_int_encoder: _VarIntEncoder = _VarIntEncoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    _var_int_encoder.encode(obj as I64, buffer)

class FloatEncoder is Encoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    buffer.f32_be(obj as F32)

class DoubleEncoder is Encoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    buffer.f64_be(obj as F64)

class BytesEncoder is Encoder
  let _long_encoder: LongEncoder = LongEncoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let data = obj as Array[U8 val] val
    _long_encoder.encode(data.size().i64(), buffer)
    buffer.write(data)

class StringEncoder is Encoder
  let _long_encoder: LongEncoder = LongEncoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let data = obj as String
    _long_encoder.encode(data.size().i64(), buffer)
    buffer.write(data.array())

class UnionEncoder is Encoder
  let _long_encoder: LongEncoder = LongEncoder
  let _encoders: Array[Encoder]
  new ref create(encoders: Array[Encoder]) =>
    _encoders = encoders
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let union = obj as Union val
    let selection = union.selection
    _long_encoder.encode(selection.i64(), buffer)
    _encoders(selection).encode(union.data, buffer)

class RecordEncoder is Encoder
  let _encoders: Array[Encoder]
  new ref create(encoders: Array[Encoder]) =>
    _encoders = encoders
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let record = obj as Record val
    for (idx, encoder) in _encoders.pairs() do
      encoder.encode(record(idx), buffer)
    end

class EnumEncoder is Encoder
  let _long_encoder: LongEncoder = LongEncoder
  let _symbols: Array[EnumSymbol val] val
  new ref create(symbols: Array[EnumSymbol val] val) =>
    _symbols = symbols
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let symbol = obj as EnumSymbol val
    _long_encoder.encode(symbol.id.i64(), buffer)

// TODO:
// Treats the array as one big array, no blocks, no skips.
class ArrayEncoder is Encoder
  let _long_encoder: LongEncoder = LongEncoder
  let _encoder: Encoder
  new ref create(encoder: Encoder) =>
    _encoder = encoder
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let array = obj as AvroArray val
    _long_encoder.encode(array.size().i64(), buffer)
    for idx in Range(0, array.size()) do
      _encoder.encode(array(idx), buffer)
    end
    _long_encoder.encode(I64(0), buffer)

// TODO:
// Treats the map as one big map, no blocks, no skips.
class MapEncoder is Encoder
  let _long_encoder: LongEncoder = LongEncoder
  let _string_encoder: StringEncoder = StringEncoder
  let _encoder: Encoder
  new ref create(encoder: Encoder) =>
    _encoder = encoder
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let map = obj as AvroMap val
    _long_encoder.encode(map.size().i64(), buffer)
    for (k, v) in map.pairs() do
      _string_encoder.encode(k, buffer)
      _encoder.encode(v, buffer)
    end
    _long_encoder.encode(I64(0), buffer)

class FixedEncoder is Encoder
  let _len: USize
  new ref create(len: USize) =>
    _len = len
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    let data = obj as Array[U8 val] val
    if data.size() != _len then error end
    buffer.write(data)

class LookupEncoder is Encoder
  let _type_string: String
  let _encoder_map: Map[String, Encoder]
  new ref create(type_string: String, map: Map[String, Encoder]) =>
    _type_string = type_string
    _encoder_map = map
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    _encoder_map(_type_string).encode(obj, buffer)

class _BogusEncoder is Encoder
  new ref create() =>
    None
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    error

class ForwardDeclarationEncoder is Encoder
  var _encoder: Encoder ref = _BogusEncoder
  new ref create() =>
    None
  fun ref set_body(encoder: Encoder) =>
    _encoder = encoder
  fun ref encode(obj: AvroType val, buffer: Writer) ? =>
    _encoder.encode(obj, buffer)
