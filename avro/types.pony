use "collections"
use "format"

type AvroType is (None | Bool | I32 | I64 | F32 | F64 | Array[U8 val] val |
                  String | Record val | EnumSymbol val | AvroArray val |
                  AvroMap val | Union val)

class EnumSymbol is (Equatable[EnumSymbol] & Stringable)
  let name: String
  let id: USize
  new val create(name': String, id': USize) =>
    name = name'
    id = id'
  fun eq(that: box->EnumSymbol): Bool =>
    (name == that.name) and (id == that.id)
  fun string(): String iso^ =>
    (name + "(" + id.string() + ")").string()

class Union
  let selection: USize
  let data: AvroType
  new create(selection': USize, data': AvroType) =>
    selection = selection'
    data = data'

class Record
  let _data: Array[AvroType] val
  new create(data: Array[AvroType] val) =>
    _data = data
  fun apply(fieldIdx: USize): AvroType val ? =>
    _data(fieldIdx)
  fun size(): USize =>
    _data.size()

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
  fun pairs(): Iterator[(String, AvroType val)] =>
    _map.pairs()
  fun size(): USize =>
    _map.size()
