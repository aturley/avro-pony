use "net"
use "collections"

type AvroType is (None | Bool | I32 | I64 | F32 | F64 | Array[U8 val] val | String |
                  Record val | EnumSymbol val | AvroArray val) // | AvroItemEnum | AvroArray | AvroMap)

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
  let _var_int_decoder: _VarIntDecoder = _VarIntDecoder
  new ref create() =>
    None
  fun ref decode(buffer: ReadBuffer): AvroType val ? =>
    let len = (LongDecoder.decode(buffer) as I64).usize()
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
        break
      | let l: I64 if l < 0 =>
        let blocks = _long_decoder.decode(buffer) as I64
        for i in Range(0, -(len.usize())) do
          array.push(_decoder.decode(buffer))
        end
      else
        break
      end
    end
    consume array

// 
// class FixedDecoder
//   let _len: USize
//   new ref create(len: USize) =>
//     _len = len
//   fun ref read(buffer: Buffer): Any val ? =>
//     buffer.block(_len)
// 
// class StringDecoder
//   fun ref read(buffer: Buffer): Any val ? =>
//     var len = (LongDecoder.read(buffer) as I64).usize()
//     let b = buffer.block(len)
//     String.from_array(consume b)
// 
// class UnionDecoder
//   let _readers: Array[Decoder]
//   new ref create(readers: Array[Decoder]) =>
//     _readers = readers
//   fun ref read(buffer: Buffer): Any val ? =>
//     var opt = (LongDecoder.read(buffer) as I64).usize()
//     _readers(opt).read(buffer)
// 
// class RecordDecoder
//   let _readers: Array[Decoder]
//   new ref create(readers: Array[Decoder]) =>
//     _readers = readers
//   fun ref read(buffer: Buffer): Any val ? =>
//     let record: Array[Any val] iso = recover Array[Any val] end
//     for (idx, reader) in _readers.pairs() do
//       record.push(reader.read(buffer))
//     end
//     consume record
// 
// class EnumDecoder
//   let _types: Array[String val] val
//   new ref create(types: Array[String val] val) =>
//     _types = types
//   fun ref read(buffer: Buffer): Any val ? =>
//     let ld: LongDecoder ref = LongDecoder
//     _types((ld.read(buffer) as I64).usize())
// 
// class TypeEnumDecoder
//   let _types: Array[Any val] val
//   new ref create(types: Array[Any val] val) =>
//     _types = types
//   fun ref read(buffer: Buffer): Any val ? =>
//     let ld: LongDecoder ref = LongDecoder
//     _types((ld.read(buffer) as I64).usize())
// 
// class ArrayDecoder
//   let _reader: Decoder
//   new ref create(reader: Decoder) =>
//     _reader = reader
//   fun ref read(buffer: Buffer): Any val ? =>
//     let ld: LongDecoder ref = LongDecoder
//     let array = recover Array[Any val] end
//     while true do
//       let len = ld.read(buffer) as I64
//       match len
//       | let l: I64 if l > 0 =>
//         for i in Range(0, len.usize()) do
//           array.push(_reader.read(buffer))
//         end
//         break
//       | let l: I64 if l < 0 =>
//         @printf[U32]("reading bytes\n".cstring())
//         let blocks = ld.read(buffer) as I64
//         @printf[U32]("read bytes\n".cstring())
//         @printf[U32]("len is (%d)\n".cstring(), len)
//         for i in Range(0, -(len.usize())) do
//           @printf[U32]("pushing\n".cstring())
//           array.push(_reader.read(buffer))
//           @printf[U32]("pushed\n".cstring())
//         end
//       else
//         @printf[U32]("break\n".cstring())
//         break
//       end
//     end
//     consume array
// 
// class MapDecoder
//   let _reader: Decoder
//   new ref create(reader: Decoder) =>
//     _reader = reader
//   fun ref read(buffer: Buffer): Any val ? =>
//     let ld: LongDecoder ref = LongDecoder
//     let sd: StringDecoder ref = StringDecoder
//     let map = recover Map[String val, Any val] end
//     while true do
//       let len = ld.read(buffer) as I64
//       match len
//       | let l: I64 if l > 0 =>
//         for i in Range(0, len.usize()) do
//           let key = sd.read(buffer) as String
//           let value = _reader.read(buffer)
//           map(consume key) = consume value
//         end
//         break
//       | let l: I64 if l < 0 =>
//         let blocks = ld.read(buffer) as I64
//         for i in Range(0, -(len.usize())) do
//           let key = sd.read(buffer) as String
//           let value = _reader.read(buffer)
//           map(consume key) = consume value
//         end
//       else
//         break
//       end
//     end
//     consume map
// 
// interface Meh
// primitive Zero is Meh
// primitive One is Meh
// primitive Two is Meh
// 
// actor Main
//   new create(env: Env) =>
//     let rb_union = Buffer.append(recover [as U8: 0x01, // array of -1 item
//                                                  0x10, // 16 bytes of data
//                                                  0x02, 0x01, // union -1
//                                                  0x02, // 1
//                                                  0x03, // -2
//                                                  0x04, // 2
//                                                  0x04, 'a', 'b', // "ab"
//                                                  0x00, // end of array
//                                                  0x04, // map of 2 items
//                                                  0x06, 'f', 'o', 'o', // "foo"
//                                                  0x04, // enum "two" (2)
//                                                  0x06, 'b', 'a', 'r', // "bar"
//                                                  0x06, // enum "three" (3)
//                                                  0x00, // enum "zero" (0)
//                                                  0x06, 0x02, 0x04, 0x00, // 3 bytes
//                                                  0x01, 0x03, 0x05, // Fixed(3)
//                                                  0x02 // Meh(1) = One
//                                                  ] end)
// 
//     let long_decoder: LongDecoder ref = LongDecoder
//     let string_decoder: StringDecoder ref = StringDecoder
//     let union_readers = [as Decoder: string_decoder, long_decoder]
// 
//     let record_readers = [as Decoder: UnionDecoder(union_readers),
//                                       long_decoder,
//                                       long_decoder,
//                                       IntDecoder,
//                                       string_decoder]
//     let rd = RecordDecoder(record_readers)
//     let ad = ArrayDecoder(rd)
//     let ed = EnumDecoder(recover ["zero", "one", "two", "three"] end)
//     let ted = TypeEnumDecoder(recover [as Meh val: Zero, One, Two] end)
//     let md = MapDecoder(ed)
//     let bd: BytesDecoder ref = BytesDecoder
//     let fd: FixedDecoder ref = FixedDecoder(3)
//     try
//       let xs = try
//         ad.read(rb_union) as Array[Any val] val
//       else
//         env.out.print("error reading array of record")
//         error
//       end
//       let x = try
//         xs(0) as Array[Any val] val
//       else
//         env.out.print("error getting item 0 of array")
//         error
//       end
//       let y = try
//         md.read(rb_union) as Map[String, Any val] val
//       else
//         env.out.print("error getting map of enum")
//         error
//       end
//       let z = try
//         ed.read(rb_union) as String
//       else
//         env.out.print("error getting enum")
//         error
//       end
//       let bx = bd.read(rb_union) as Array[U8 val] val
//       let fx = fd.read(rb_union) as Array[U8 val] val
//       let tx = ted.read(rb_union) as Meh val
// 
//       env.out.print((x(0) as I64 val).string())
//       env.out.print((x(1) as I64 val).string())
//       env.out.print((x(2) as I64 val).string())
//       env.out.print((x(3) as I32 val).string())
//       env.out.print(x(4) as String val)
//       env.out.print(y("foo") as String val)
//       env.out.print(y("bar") as String val)
//       env.out.print(z.string())
//       env.out.print(bx.size().string())
//       env.out.print(fx.size().string())
//       env.out.print(match tx
//                     | Zero => "zero"
//                     | One => "one"
//                     | Two => "two"
//                     else
//                       "meh?"
//                     end)
//     else
//       env.out.print("error with data")
//     end