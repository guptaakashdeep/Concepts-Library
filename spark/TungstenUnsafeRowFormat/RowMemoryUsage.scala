package org.sparkdeepdive.application

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{StringType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.Platform

object RowMemoryUsage  extends App {
  val schema = Array(StringType, IntegerType, StringType, StringType)
//val schema = Array(IntegerType, StringType, StringType, StringType)
  val row = new SpecificInternalRow(schema)

  val testString = "data"
  val t1 = "eng"
  val utf8String = UTF8String.fromString(testString)
  val utf8String2 = UTF8String.fromString(t1)
  row.update(0, utf8String)
  row.update(1, 999)
  row.update(2, utf8String2)

  // Test2
//  val testString = "data"
//  val t1 = "bricks"
//  val utf8String = UTF8String.fromString(testString)
//  val utf8String2 = UTF8String.fromString(t1)
//  row.update(1, utf8String)
//  row.update(0, 123)
//  row.update(2, utf8String2)
//  row.update(3, 10)
  row.setNullAt(3)

  val attrs = schema.zipWithIndex.map { case (dt, i) =>
    AttributeReference(s"c$i", dt)()
  }

  val converter = UnsafeProjection.create(attrs, attrs)
  val unsafeRow = converter.apply(row)

  // Memory layout details
  println(s"Total size in bytes: ${unsafeRow.getSizeInBytes}")
  println(s"Number of fields: ${unsafeRow.numFields()}")
  println(s"Null bit set size: ${UnsafeRow.calculateBitSetWidthInBytes(unsafeRow.numFields())} bytes")
  println(s"Field offset size: 8 bytes") // Each variable-length field needs 8 bytes for offset
//  println(s"String value size: ${unsafeRow.getUTF8String(0).numBytes()} bytes")
//  println(s"Integer value size: ${unsafeRow.getInt(1).numBytes()} bytes")

  // If you want to see the actual offset where the string is stored
  val baseOffset = unsafeRow.getBaseOffset
  println(s"String offset in the row: $baseOffset")
  println(s"Platform Byte_OFFSET: ${Platform.BYTE_ARRAY_OFFSET} bytes")

  //val nullBitSetSize = UnsafeRow.calculateBitSetWidthInBytes(unsafeRow.numFields())

  // Try reading the actual integer value at different positions
  // Get the actual base object
  val baseObject = unsafeRow.getBaseObject
  println("\nTrying different offsets:")
  (0 until 10).foreach { i =>
    val offset = baseOffset + (i * 4)
    try {
      val value = Platform.getInt(baseObject, offset)
      println(f"Offset ${offset}%d (0x${offset}%x): $value")
    } catch {
      case e: Exception =>
        println(f"Error reading at offset ${offset}%d: ${e.getMessage}")
    }
  }

  // To get integer value (4 bytes)
  val intValue = Platform.getInt(baseObject, baseOffset + 16) // should get 20

  // To get string value (since we know length is 4 for "abcd")
  val stringBytes = new Array[Byte](4)
  Platform.copyMemory(
    baseObject,                // source object
    baseOffset + 40,          // source offset where "data" starts
    stringBytes,              // destination array
    Platform.BYTE_ARRAY_OFFSET, // destination offset
    4                         // length to copy
  )
  val firstString = new String(stringBytes) // should get "data"

  // For second string "l" (length 1)
  val string2Bytes = new Array[Byte](3)
  Platform.copyMemory(
    baseObject,
    baseOffset + 48,          // where "engg" starts
    string2Bytes,
    Platform.BYTE_ARRAY_OFFSET,
    3
  )
  val secondString = new String(string2Bytes) // should get "l"

  println(s"First string: $firstString")
  println(s"int Value: $intValue")
  println(s"Second string: $secondString")

}
