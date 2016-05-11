use "net"
use "collections"

class AvroDecoder
  let _read_buffer: Buffer
  new create(read_buffer: Buffer) =>
    _read_buffer = read_buffer
  fun null(): None =>
    None
  fun ref boolean(): Bool ? =>
    _read_buffer.u8() != 0
  fun ref int(): I32 ? =>
    var acc: I32 = 0
    var b = _read_buffer.u8().i32()
    repeat
      acc = (acc << 8) + (b >> 1)
    until (b and 0x80) == 0 end
    acc
  fun ref long(): I64 ? =>
    var acc: I64 = 0
    var b = _read_buffer.u8().i64()
    repeat
      acc = (acc << 8) + (b >> 1)
    until (b and 0x80) == 0 end
    acc
  fun ref float(): F32 ? =>
    _read_buffer.f32_le()
  fun ref double(): F64 ? =>
    _read_buffer.f64_le()
  fun ref bytes(): Array[U8] val ? =>
    var len = int().usize()
    _read_buffer.block(len)
  fun ref string(): String val ? =>
    var len = int().usize()
    let b = _read_buffer.block(len)
    String.from_array(consume b)
  fun ref union(readers: Array[{(): Any val} ref]): Any val ? =>
    var opt = int().usize()
    readers(opt)()

class VarIntDecoder
  fun ref read(buffer: Buffer): I64 val ? =>
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

interface Decoder[A]
  fun ref read(buffer: Buffer): A ?

class IntDecoder[A: I32 val = I32 val]
  let _var_int_decoder: VarIntDecoder = VarIntDecoder
  fun ref read(buffer: Buffer): A ? =>
    (_var_int_decoder.read(buffer) as I64).i32()

class LongDecoder[A: I64 val = I64 val]
  let _var_int_decoder: VarIntDecoder = VarIntDecoder
  fun ref read(buffer: Buffer): A ? =>
    _var_int_decoder.read(buffer) as I64

class StringDecoder[A: String val = String val]
  fun ref read(buffer: Buffer): A ? =>
    var len = LongDecoder.read(buffer).usize()
    let b = buffer.block(len)
    String.from_array(consume b)

class UnionDecoder[A, B: Decoder[A]]
  let _readers: Array[B]
  new ref create(readers: Array[B]) =>
    _readers = readers
  fun ref read(buffer: Buffer): A ? =>
    var opt = (LongDecoder.read(buffer) as I64).usize()
    _readers(opt).read(buffer)

// class RecordDecoder
//   let _readers: Array[Decoder]
//   new ref create(readers: Array[Decoder]) =>
//     _readers = readers
//   fun ref read(buffer: Buffer): Any val ? =>
//     let record: Array[Any] iso = recover Array[Any] end
//     for (idx, reader) in _readers.pairs() do
//       record.push(reader.read(buffer))
//     end
//     consume record
// 
// class ArrayDecoder
//   let _reader: Decoder
//   new ref create(reader: Decoder) =>
//     _reader = reader
//   fun ref read(buffer: Buffer): Any val ? =>
//     let ld: LongDecoder ref = LongDecoder
//     let array = recover Array[Any] end
//     while true do
//       let len = ld.read(buffer) as I64
//       match len
//       | let l: I64 if l > 0 =>
//         for i in Range(0, len.usize()) do
//           array.push(_reader.read(buffer))
//         end
//       | let l: I64 if l < 0 =>
//         let blocks = ld.read(buffer) as I64
//         for i in Range(0, -(len.usize())) do
//           array.push(_reader.read(buffer))
//         end
//       else
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
//     let map = recover Map[String, Any] end
//     while true do
//       let len = ld.read(buffer) as I64
//       match len
//       | let l: I64 if l > 0 =>
//         for i in Range(0, len.usize()) do
//           let key = sd.read(buffer) as String
//           let value = _reader.read(buffer)
//           map(consume key) = consume value
//         end
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
actor Main
  new create(env: Env) =>
    let rb_union = Buffer.append(recover [as U8: 0x02, 0x01, 0x02, 0x03, 0x04, 0x04, 'a', 'b'] end)

    let long_decoder: LongDecoder ref = LongDecoder
    let string_decoder: StringDecoder ref = StringDecoder
    let union_readers = [as Decoder[(String | I64)] ref: string_decoder, long_decoder]

    try
      let x = UnionDecoder[(String | I64)](union_readers).read(rb_union) as I64
      env.out.print(x.string())
    else
      env.out.print("error")
    end
// 
//     for i in Range(0, 3) do
//       try
//         let x = long_decoder.read(rb_union) as I64
//         env.out.print(x.string())
//       else
//         env.out.print("error")
//       end
//     end
// 
//     try
//       let x = string_decoder.read(rb_union) as String
//       env.out.print(x)
//     else
//       env.out.print("error")
//     end
