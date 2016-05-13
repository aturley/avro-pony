use "ponytest"
use "collections"
use "net"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)
  new make() => None
  fun tag tests(test: PonyTest) =>
    test(_TestNone)
    test(_TestBoolean)
    test(_TestInt)
    test(_TestLong)
    test(_TestFloat)
    test(_TestDouble)
    test(_TestBytes)
    test(_TestString)
    test(_TestUnion)
    test(_TestRecord)

class iso _TestNone is UnitTest
  fun name(): String => "avro/NoneDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_none = ReadBuffer.append(recover Array[U8 val] end) // nothing
    let none_decoder = NoneDecoder
    h.assert_eq[None](None, none_decoder.decode(rb_none) as None)

class iso _TestBoolean is UnitTest
  fun name(): String => "avro/BooleanDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_boolean = ReadBuffer.append(recover [as U8: 1, 0] end) // True, False
    let boolean_decoder = BooleanDecoder
    h.assert_eq[Bool](true, boolean_decoder.decode(rb_boolean) as Bool)
    h.assert_eq[Bool](false, boolean_decoder.decode(rb_boolean) as Bool)

class iso _TestInt is UnitTest
  fun name(): String => "avro/IntDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_int = ReadBuffer.append(recover [as U8: 0x01] end) // -1
    let int_decoder = IntDecoder
    h.assert_eq[I32](-1, int_decoder.decode(rb_int) as I32)

class iso _TestLong is UnitTest
  fun name(): String => "avro/LongDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_long = ReadBuffer.append(recover [as U8: 0x01] end) // -1
    let long_decoder = LongDecoder
    h.assert_eq[I64](-1, long_decoder.decode(rb_long) as I64)

class iso _TestFloat is UnitTest
  fun name(): String => "avro/FloatDecoder"

  fun apply(h: TestHelper) ? =>
    let wb_float = WriteBuffer
    wb_float.f32_be(3.14159)
    let rb_float = ReadBuffer.append(wb_float.done()(0) as Array[U8] val)
    let float_decoder = FloatDecoder
    h.assert_eq[F32](3.14159, float_decoder.decode(rb_float) as F32)

class iso _TestDouble is UnitTest
  fun name(): String => "avro/DoubleDecoder"

  fun apply(h: TestHelper) ? =>
    let wb_double = WriteBuffer
    wb_double.f64_be(3.14159)
    let rb_double = ReadBuffer.append(wb_double.done()(0) as Array[U8] val)
    let double_decoder = DoubleDecoder
    h.assert_eq[F64](3.14159, double_decoder.decode(rb_double) as F64)

primitive _AssertArrayEqU8
  fun apply(h: TestHelper, a1: Array[U8 val] val, a2: Array[U8 val] val) ? =>
    if a1.size() != a2.size() then
      h.fail("Array sizes differ: " + a1.size().string() + " != " +
             a2.size().string())
    else
      for (idx, v) in a1.pairs() do
        if v != a2(idx) then
          h.fail("At index " + idx.string() + " " + a1(idx).string() + " != " + a2(idx).string())
        end
      end
    end

class iso _TestBytes is UnitTest
  fun name(): String => "avro/BytesDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x08, // 4
                               0x0B, 0x0E, // 0x0B 0x0E
                               0x0E, 0x0F] end // 0x0E, 0x0F
    let rb_bytes = ReadBuffer.append(consume data)
    let bytes_decoder = BytesDecoder
    _AssertArrayEqU8(h, recover [as U8 val: 0x0B, 0x0E, 0x0E, 0x0F] end,
                        bytes_decoder.decode(rb_bytes) as Array[U8 val] val)

class iso _TestString is UnitTest
  fun name(): String => "avro/StringDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x06, 'a', 'b', 'c'] end // "abc"
    let rb_string = ReadBuffer.append(consume data)
    let string_decoder = StringDecoder
    h.assert_eq[String]("abc", string_decoder.decode(rb_string) as String)

class iso _TestUnion is UnitTest
  fun name(): String => "avro/UnionDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x02, 0x06, 'a', 'b', 'c'] end // 1, "abc"
    let rb_union = ReadBuffer.append(consume data)
    let int_decoder = IntDecoder
    let string_decoder = StringDecoder
    let union_decoder = UnionDecoder([as Decoder: int_decoder, string_decoder])
    h.assert_eq[String]("abc", union_decoder.decode(rb_union) as String)

class iso _TestRecord is UnitTest
  fun name(): String => "avro/RecordDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x02, 0x06, 'a', 'b', 'c'] end // 1, "abc"
    let rb_record = ReadBuffer.append(consume data)
    let int_decoder = IntDecoder
    let string_decoder = StringDecoder
    let record_decoder = RecordDecoder([as Decoder: int_decoder, string_decoder])
    let record = record_decoder.decode(rb_record) as Record val
    h.assert_eq[I32](1, record(0) as I32)
    h.assert_eq[String]("abc", record(1) as String)
