use "ponytest"
use "collections"
use "net"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)
  new make() => None
  fun tag tests(test: PonyTest) =>
    test(_TestVarIntEncoder)
    test(_TestVarIntDecoder)

    test(_TestNoneDecoder)
    test(_TestBooleanDecoder)
    test(_TestIntDecoder)
    test(_TestLongDecoder)
    test(_TestFloatDecoder)
    test(_TestDoubleDecoder)
    test(_TestBytesDecoder)
    test(_TestStringDecoder)
    test(_TestUnionDecoder)
    test(_TestRecordDecoder)
    test(_TestEnumDecoder)
    test(_TestArrayDecoder)
    test(_TestMapDecoder)
    test(_TestFixedDecoder)

    test(_TestNoneEncoder)
    test(_TestBooleanEncoder)
    test(_TestIntEncoder)
    test(_TestLongEncoder)
    test(_TestFloatEncoder)
    test(_TestDoubleEncoder)
    test(_TestBytesEncoder)
    test(_TestStringEncoder)
    test(_TestUnionEncoder)
    test(_TestRecordEncoder)
    test(_TestEnumEncoder)
    test(_TestArrayEncoder)
    test(_TestMapEncoder)
    test(_TestFixedEncoder)

class iso _TestVarIntEncoder is UnitTest
  fun name(): String => "avro/_VarIntEncoder"

  fun apply(h: TestHelper) ? =>
    let var_int_encoder = _VarIntEncoder
    let var_int_decoder = _VarIntDecoder
    for control in [as I64:
                    0x00,
                    -0x01, 0x01,
                    -0x02, 0x02,
                    -0xFE, 0xFE,
                    -0x1FE, 0x1FE,
                    -0xFEDCA, 0xFEDCA,
                    -0xFEDCA98, 0xFEDCA98].values() do
      try
        let wb: WriteBuffer ref = WriteBuffer
        var_int_encoder.encode(control, wb)
        let rb_wb: ReadBuffer ref = ReadBuffer
        rb_wb.append((wb.done()(0) as Array[U8 val] val))
        let actual = var_int_decoder.decode(rb_wb) as I64
        h.assert_eq[I64](control, actual,
                         "wb -> rb failed control=" + control.string())
      else
        h.fail("Error when control=" + control.string())
        error
      end
    end

class iso _TestVarIntDecoder is UnitTest
  fun name(): String => "avro/_VarIntDecoder"

  fun apply(h: TestHelper) ? =>
    let var_int_decoder = _VarIntDecoder
    let var_int_encoder = _VarIntEncoder
    for control in [as I64:
                    0x00,
                    0x01,
                    0x02,
                    0xFE,
                    0xFF,
                    0x1FF,
                    0x1FE,
                    0xFEDCB,
                    0xFEDCA,
                    0xFEDCBA9,
                    0xFEDCBA8].values() do
      try
        let rb: ReadBuffer ref = ReadBuffer
        let b0 = control.u8() and 0x7F
        let b1 = (control >> 7).u8() and 0x7F
        let b2 = (control >> 14).u8() and 0x7F
        let b3 = (control >> 21).u8() and 0x7F
        if (b1 == 0) and (b2 == 0) and (b3 == 0) then
          rb.append(recover [b0] end)
        elseif (b2 == 0) and (b3 == 0) then
          rb.append(recover [b0 or (1 << 7), b1] end)
        elseif (b3 == 0) then
          rb.append(recover [b0 or (1 << 7), b1 or (1 << 7), b2] end)
        else
          rb.append(recover [b0 or (1 << 7), b1 or (1 << 7), b2 or (1 << 7), b3] end)
        end
        let int = var_int_decoder.decode(rb) as I64
        let expected = ((control + 1) / 2) *
                       (if (control % 2) == 0 then 1 else -1 end)
        h.assert_eq[I64](expected, int, expected.string() + " != " +
                         int.string() + " for control=" + control.string())
      else
        h.fail("Error when control=" + control.string())
        error
      end
    end

class iso _TestNoneDecoder is UnitTest
  fun name(): String => "avro/NoneDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_none = ReadBuffer.append(recover Array[U8 val] end) // nothing
    let none_decoder = NoneDecoder
    h.assert_eq[None](None, none_decoder.decode(rb_none) as None)

class iso _TestBooleanDecoder is UnitTest
  fun name(): String => "avro/BooleanDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_boolean = ReadBuffer.append(recover [as U8: 1, 0] end) // True, False
    let boolean_decoder = BooleanDecoder
    h.assert_eq[Bool](true, boolean_decoder.decode(rb_boolean) as Bool)
    h.assert_eq[Bool](false, boolean_decoder.decode(rb_boolean) as Bool)

class iso _TestIntDecoder is UnitTest
  fun name(): String => "avro/IntDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_int = ReadBuffer.append(recover [as U8: 0x01] end) // -1
    let int_decoder = IntDecoder
    h.assert_eq[I32](-1, int_decoder.decode(rb_int) as I32)

class iso _TestLongDecoder is UnitTest
  fun name(): String => "avro/LongDecoder"

  fun apply(h: TestHelper) ? =>
    let rb_long = ReadBuffer.append(recover [as U8: 0x01] end) // -1
    let long_decoder = LongDecoder
    h.assert_eq[I64](-1, long_decoder.decode(rb_long) as I64)

class iso _TestFloatDecoder is UnitTest
  fun name(): String => "avro/FloatDecoder"

  fun apply(h: TestHelper) ? =>
    let wb_float = WriteBuffer
    wb_float.f32_be(3.14159)
    let rb_float = ReadBuffer.append(wb_float.done()(0) as Array[U8] val)
    let float_decoder = FloatDecoder
    h.assert_eq[F32](3.14159, float_decoder.decode(rb_float) as F32)

class iso _TestDoubleDecoder is UnitTest
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

class iso _TestBytesDecoder is UnitTest
  fun name(): String => "avro/BytesDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x08, // 4
                               0x0B, 0x0E, // 0x0B 0x0E
                               0x0E, 0x0F] end // 0x0E, 0x0F
    let rb_bytes = ReadBuffer.append(consume data)
    let bytes_decoder = BytesDecoder
    _AssertArrayEqU8(h, recover [as U8 val: 0x0B, 0x0E, 0x0E, 0x0F] end,
                     bytes_decoder.decode(rb_bytes) as Array[U8 val] val)

class iso _TestStringDecoder is UnitTest
  fun name(): String => "avro/StringDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x06, 'a', 'b', 'c'] end // "abc"
    let rb_string = ReadBuffer.append(consume data)
    let string_decoder = StringDecoder
    h.assert_eq[String]("abc", string_decoder.decode(rb_string) as String)

class iso _TestUnionDecoder is UnitTest
  fun name(): String => "avro/UnionDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x02, 0x06, 'a', 'b', 'c'] end // 1, "abc"
    let rb_union = ReadBuffer.append(consume data)
    let int_decoder = IntDecoder
    let string_decoder = StringDecoder
    let union_decoder = UnionDecoder([as Decoder: int_decoder, string_decoder])
    let actual = union_decoder.decode(rb_union) as Union val
    h.assert_eq[USize](1, actual.selection as USize)
    h.assert_eq[String]("abc",  actual.data as String)

class iso _TestRecordDecoder is UnitTest
  fun name(): String => "avro/RecordDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x02, 0x06, 'a', 'b', 'c'] end // 1, "abc"
    let rb_record = ReadBuffer.append(consume data)
    let int_decoder = IntDecoder
    let string_decoder = StringDecoder
    let record_decoder = RecordDecoder(
      [as Decoder: int_decoder, string_decoder])
    let record = record_decoder.decode(rb_record) as Record val
    h.assert_eq[I32](1, record(0) as I32)
    h.assert_eq[String]("abc", record(1) as String)

class iso _TestEnumDecoder is UnitTest
  fun name(): String => "avro/EnumDecoder"
  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x02] end // 1 (EnumOne)
    let rb_enum = ReadBuffer.append(consume data)
    let enum_zero = EnumSymbol("zero", 0)
    let enum_one = EnumSymbol("one", 1)
    let enum_two = EnumSymbol("two", 2)
    let enum_decoder = EnumDecoder(recover [enum_zero, enum_one, enum_two] end)
    let enum_symbol = enum_decoder.decode(rb_enum) as EnumSymbol val
    if enum_symbol != enum_one then
      h.fail("enum_symbol isn't enum_one")
    end

class iso _TestArrayDecoder is UnitTest
  fun name(): String => "avro/ArrayDecoder"
  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x04, // 2 items
                               0x06, 'r', 'a', 't', // "abc"
                               0x08, 'b', 'e', 'a', 'r', // "bear"
                               0x02, // 1 item
                               0x04, 'o', 'x', // "ox"
                               0x00] end  // 0 (end list)
    let rb_array = ReadBuffer.append(consume data)
    let string_decoder = StringDecoder
    let array_decoder = ArrayDecoder(string_decoder)
    let array = array_decoder.decode(rb_array) as AvroArray val
    h.assert_eq[USize](3, array.size())
    h.assert_eq[String]("rat", array(0) as String)
    h.assert_eq[String]("bear", array(1) as String)
    h.assert_eq[String]("ox", array(2) as String)

class iso _TestMapDecoder is UnitTest
  fun name(): String => "avro/MapDecoder"
  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x04, // 2 items
                               0x06, 'r', 'a', 't', 0x10, // "abc": 8
                               0x08, 'b', 'e', 'a', 'r', 0x12, // "bear": 9
                               0x02, // 1 item
                               0x04, 'o', 'x', 0x14, // "ox": 10
                               0x00] end  // 0 (end list)
    let rb_map = ReadBuffer.append(consume data)
    let long_decoder = LongDecoder
    let map_decoder = MapDecoder(long_decoder)
    let map = map_decoder.decode(rb_map) as AvroMap val
    h.assert_eq[I64](8, map("rat") as I64)
    h.assert_eq[I64](9, map("bear") as I64)
    h.assert_eq[I64](10, map("ox") as I64)

class iso _TestFixedDecoder is UnitTest
  fun name(): String => "avro/FixedDecoder"

  fun apply(h: TestHelper) ? =>
    let data = recover [as U8: 0x0B, 0x0E, // 0x0B 0x0E
                               0x0E, 0x0F] end // 0x0E, 0x0F
    let rb_fixed = ReadBuffer.append(consume data)
    let fixed_decoder = FixedDecoder(4)
    _AssertArrayEqU8(h, recover [as U8 val: 0x0B, 0x0E, 0x0E, 0x0F] end,
                     fixed_decoder.decode(rb_fixed) as Array[U8 val] val)

class iso _TestNoneEncoder is UnitTest
  fun name(): String => "avro/NoneEncoder"

  fun apply(h: TestHelper) ? =>
    let none_encoder = NoneEncoder
    let none_decoder = NoneDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected = None

    none_encoder.encode(expected, wb)
    let actual = none_decoder.decode(rb) as None
    h.assert_eq[None](expected, actual)

class iso _TestBooleanEncoder is UnitTest
  fun name(): String => "avro/BooleanEncoder"

  fun apply(h: TestHelper) ? =>
    let boolean_encoder = BooleanEncoder
    let boolean_decoder = BooleanDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected = true

    boolean_encoder.encode(expected, wb)
    rb.append(wb.done()(0) as Array[U8 val] val)
    let actual = boolean_decoder.decode(rb) as Bool
    h.assert_eq[Bool](expected, actual)

class iso _TestIntEncoder is UnitTest
  fun name(): String => "avro/IntEncoder"

  fun apply(h: TestHelper) ? =>
    let int_encoder = IntEncoder
    let int_decoder = IntDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: I32 = 3000

    int_encoder.encode(expected, wb)
    rb.append(wb.done()(0) as Array[U8 val] val)
    let actual = int_decoder.decode(rb) as I32
    h.assert_eq[I32](expected, actual)

class iso _TestLongEncoder is UnitTest
  fun name(): String => "avro/LongEncoder"

  fun apply(h: TestHelper) ? =>
    let long_encoder = LongEncoder
    let long_decoder = LongDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: I64 = 3000

    long_encoder.encode(expected, wb)
    rb.append(wb.done()(0) as Array[U8 val] val)
    let actual = long_decoder.decode(rb) as I64
    h.assert_eq[I64](expected, actual)

class iso _TestFloatEncoder is UnitTest
  fun name(): String => "avro/FloatEncoder"

  fun apply(h: TestHelper) ? =>
    let float_encoder = FloatEncoder
    let float_decoder = FloatDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: F32 = 3.14159

    float_encoder.encode(expected, wb)
    rb.append(wb.done()(0) as Array[U8 val] val)
    let actual = float_decoder.decode(rb) as F32
    h.assert_eq[F32](expected, actual)

class iso _TestDoubleEncoder is UnitTest
  fun name(): String => "avro/DoubleEncoder"

  fun apply(h: TestHelper) ? =>
    let double_encoder = DoubleEncoder
    let double_decoder = DoubleDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: F64 = 314159.314159

    double_encoder.encode(expected, wb)
    rb.append(wb.done()(0) as Array[U8 val] val)
    let actual = double_decoder.decode(rb) as F64
    h.assert_eq[F64](expected, actual)

class iso _TestBytesEncoder is UnitTest
  fun name(): String => "avro/BytesEncoder"

  fun apply(h: TestHelper) ? =>
    let bytes_encoder = BytesEncoder
    let bytes_decoder = BytesDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: Array[U8 val] val = recover
      [as U8 val: 0xDE, 0xAD, 0xBE, 0xEF] end

    bytes_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = bytes_decoder.decode(rb) as Array [U8] val
    _AssertArrayEqU8(h, expected, actual)

class iso _TestStringEncoder is UnitTest
  fun name(): String => "avro/StringEncoder"

  fun apply(h: TestHelper) ? =>
    let string_encoder = StringEncoder
    let string_decoder = StringDecoder
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: String = "hello world\nbye"

    string_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = string_decoder.decode(rb) as String val
    h.assert_eq[String](expected, actual)

class iso _TestUnionEncoder is UnitTest
  fun name(): String => "avro/UnionEncoder"

  fun apply(h: TestHelper) ? =>
    let string_encoder = StringEncoder
    let int_encoder = IntEncoder
    let union_encoder = UnionEncoder([as Encoder: int_encoder, string_encoder])
    let int_decoder = IntDecoder
    let string_decoder = StringDecoder
    let union_decoder = UnionDecoder([as Decoder: int_decoder, string_decoder])
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: Union val = recover Union(1, "hello world") end

    union_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = union_decoder.decode(rb) as Union val
    h.assert_eq[USize](expected.selection as USize, actual.selection as USize)
    h.assert_eq[String](expected.data as String val, actual.data as String val)

class iso _TestRecordEncoder is UnitTest
  fun name(): String => "avro/RecordEncoder"

  fun apply(h: TestHelper) ? =>
    let str_encoder = StringEncoder
    let int_encoder = IntEncoder
    let record_encoder = RecordEncoder([as Encoder: int_encoder, str_encoder])
    let int_decoder = IntDecoder
    let str_decoder = StringDecoder
    let record_decoder = RecordDecoder([as Decoder: int_decoder, str_decoder])
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: Record val = recover Record(recover
      [as AvroType: I32(42), "meaning of life"] end) end

    record_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = record_decoder.decode(rb) as Record val
    h.assert_eq[USize](expected.size() as USize, actual.size() as USize)
    h.assert_eq[I32](expected(0) as I32 val, actual(0) as I32 val)
    h.assert_eq[String](expected(1) as String val, actual(1) as String val)

class iso _TestEnumEncoder is UnitTest
  fun name(): String => "avro/EnumEncoder"

  fun apply(h: TestHelper) ? =>
    let enum_zero = EnumSymbol("zero", 0)
    let enum_one = EnumSymbol("one", 1)
    let enum_two = EnumSymbol("two", 2)
    let symbols: Array[EnumSymbol val] val = recover
      [enum_zero, enum_one, enum_two]
    end
    let enum_encoder = EnumEncoder(symbols)
    let enum_decoder = EnumDecoder(symbols)
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected = enum_one

    enum_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = enum_decoder.decode(rb) as EnumSymbol val
    h.assert_eq[EnumSymbol val](expected, actual)

class iso _TestArrayEncoder is UnitTest
  fun name(): String => "avro/ArrayEncoder"

  fun apply(h: TestHelper) ? =>
    let string_encoder = StringEncoder
    let string_decoder = StringDecoder
    let array_encoder = ArrayEncoder(string_encoder)
    let array_decoder = ArrayDecoder(string_decoder)
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: AvroArray val = recover
      AvroArray(recover [as AvroType: "a", "b", "c"] end)
    end

    array_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = array_decoder.decode(rb) as AvroArray val
    h.assert_eq[USize](expected.size(), actual.size())
    h.assert_eq[String](expected(0) as String, actual(0) as String)
    h.assert_eq[String](expected(1) as String, actual(1) as String)
    h.assert_eq[String](expected(2) as String, actual(2) as String)

class iso _TestMapEncoder is UnitTest
  fun name(): String => "avro/MapEncoder"

  fun apply(h: TestHelper) ? =>
    let string_encoder = StringEncoder
    let string_decoder = StringDecoder
    let map_encoder = MapEncoder(string_encoder)
    let map_decoder = MapDecoder(string_decoder)
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: AvroMap val = recover
      AvroMap(recover
        let map = recover Map[String val, AvroType val] end
        map.update("a", "A")
        map.update("b", "B")
        map.update("c", "C")
        consume map
      end)
    end

    map_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = map_decoder.decode(rb) as AvroMap val
    h.assert_eq[String](expected("a") as String val, actual("a") as String val)
    h.assert_eq[String](expected("b") as String val, actual("b") as String val)
    h.assert_eq[String](expected("c") as String val, actual("c") as String val)

class iso _TestFixedEncoder is UnitTest
  fun name(): String => "avro/FixedEncoder"

  fun apply(h: TestHelper) ? =>
    let fixed_encoder = FixedEncoder
    let fixed_decoder = FixedDecoder(4)
    let wb: WriteBuffer ref = WriteBuffer
    let rb: ReadBuffer ref = ReadBuffer
    let expected: Array[U8 val] val = recover
      [as U8 val: 0xDE, 0xAD, 0xBE, 0xEF] end

    fixed_encoder.encode(expected, wb)
    for x in wb.done().values() do
      rb.append(x as Array[U8 val] val)
    end
    let actual = fixed_decoder.decode(rb) as Array [U8] val
    _AssertArrayEqU8(h, expected, actual)
