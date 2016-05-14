use "net"
use "collections"

type AvroType is (None | Bool | I32 | I64 | F32 | F64 | Array[U8 val] val |
                  String | Record val | EnumSymbol val | AvroArray val |
                  AvroMap val)

interface EnumSymbol

class Record
  let _fields: Array[AvroType] val
  new create(fields: Array[AvroType] val) =>
    _fields = fields
  fun apply(fieldIdx: USize): AvroType val ? =>
    _fields(fieldIdx)

class AvroArray
  let _array: Array[AvroType] val
  new create(array: Array[AvroType] val) =>
    _array = array
  fun apply(idx: USize): AvroType val ? =>
    _array(idx)
  fun size(): USize =>
    _array.size()

class AvroMap
  let _map: Map[String val, AvroType val] val
  new create(map: Map[String val, AvroType val] val) =>
    _map = map
  fun apply(key: String): AvroType val ? =>
    _map(key)

interface Decoder
  fun ref decode(buffer: ReadBuffer): AvroType val ?

class NoneDecoder is Decoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val =>
    None

class BooleanDecoder is Decoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    buffer.u8() != 0

class _VarIntDecoder is Decoder
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    var acc: I64 = 0
    var shift: I64 = 0

    var b: I64 = 0
    while true do
      b = I64.from[U8](buffer.u8())
      if (b and 0x80) == 0 then
        break
      end
      acc = acc or ((b and 0x7F) << shift)
      shift = shift + 7
    end

    acc = acc or ((b and 0x7F) << shift)
    if (acc and 1) == 1 then
      (acc >> 1) xor -1
    else
      (acc >> 1)
    end

class IntDecoder is Decoder
  let _var_int_decoder: _VarIntDecoder = _VarIntDecoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    (_var_int_decoder.decode(buffer) as I64).i32()

class LongDecoder is Decoder
  let _var_int_decoder: _VarIntDecoder = _VarIntDecoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    _var_int_decoder.decode(buffer) as I64

class FloatDecoder is Decoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    buffer.f32_be()

class DoubleDecoder is Decoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    buffer.f64_be()

class BytesDecoder is Decoder
  let _long_decoder: LongDecoder = LongDecoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    let len = (_long_decoder.decode(buffer) as I64).usize()
    buffer.block(len)

class StringDecoder is Decoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    var len = (LongDecoder.decode(buffer) as I64).usize()
    let b = buffer.block(len)
    String.from_array(consume b)

class UnionDecoder is Decoder
  let _decoders: Array[Decoder]
  new ref create(decoders: Array[Decoder]) =>
    _decoders = decoders
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    var opt = (LongDecoder.decode(buffer) as I64).usize()
    _decoders(opt).decode(buffer)

class RecordDecoder is Decoder
  let _decoders: Array[Decoder]
  new ref create(decoders: Array[Decoder]) =>
    _decoders = decoders
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    let record: Array[AvroType val] iso = recover Array[AvroType val] end
    for decoder in _decoders.values() do
       record.push(decoder.decode(buffer))
    end
    recover Record(consume record) end

class EnumDecoder is Decoder
  let _long_decoder: LongDecoder = LongDecoder
  let _enum_symbols: Array[EnumSymbol val] val
  new ref create(enum_symbols: Array[EnumSymbol val] val) =>
    _enum_symbols = enum_symbols
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    let idx = (_long_decoder.decode(buffer) as I64).usize()
    _enum_symbols(idx)

class ArrayDecoder
  let _long_decoder: LongDecoder = LongDecoder
  let _decoder: Decoder
  new ref create(decoder: Decoder) =>
    _decoder = decoder
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    let array = recover Array[AvroType val] end
    while true do
      let len = _long_decoder.decode(buffer) as I64
      match len
      | let l: I64 if l > 0 =>
        for i in Range(0, len.usize()) do
          array.push(_decoder.decode(buffer))
        end
      | let l: I64 if l < 0 =>
        let blocks = _long_decoder.decode(buffer) as I64
        for i in Range(0, -(len.usize())) do
          array.push(_decoder.decode(buffer))
        end
      else
        break
      end
    end
    recover AvroArray(consume array) end

class MapDecoder
  let _long_decoder: LongDecoder = LongDecoder
  let _decoder: Decoder
  new ref create(decoder: Decoder) =>
    _decoder = decoder
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    let sd: StringDecoder ref = StringDecoder
    let map = recover Map[String val, AvroType val] end
    while true do
      let len = _long_decoder.decode(buffer) as I64
      match len
      | let l: I64 if l > 0 =>
        for i in Range(0, len.usize()) do
          let key = sd.decode(buffer) as String
          let value = _decoder.decode(buffer)
          map(consume key) = consume value
        end
      | let l: I64 if l < 0 =>
        let blocks = _long_decoder.decode(buffer) as I64
        for i in Range(0, -(len.usize())) do
          let key = sd.decode(buffer) as String
          let value = _decoder.decode(buffer)
          map(consume key) = consume value
        end
      else
        break
      end
    end
    recover AvroMap(consume map) end

class FixedDecoder is Decoder
  let _len: USize
  new ref create(len: USize) =>
    _len = len
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    buffer.block(_len)
