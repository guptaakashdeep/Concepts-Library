package org.sparkdeepdive.application

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.openjdk.jol.info.{ClassLayout, GraphLayout}

// Java class to match our UnsafeRow structure
class TestData {
  var str1: String = "abcd"
  var num: Int = 20
  var str2: String = "l"
  var str3: String = null // to match our null value
}

object JavaVsUnsafeRowMemoryComparison extends App { //
  val javaStr = "abcd"
  //  println("Java String Memory Layout:")
  //  println(ClassLayout.parseInstance(javaStr).toPrintable())
  println(s"Java String total size: ${GraphLayout.parseInstance(javaStr).totalSize()} bytes")
  println("-" * 50)

  // Create test row
  val strRow = InternalRow.fromSeq(Seq(
    UTF8String.fromString("data"),
  ))

  val expressions = Seq(
    BoundReference(0, StringType, nullable = true)
  )

  val projection = UnsafeProjection.create(expressions)
  // Apply projection and get result
  val unsafeRow_str = projection.apply(strRow)
  println(s"Unsafe Row Size: ${unsafeRow_str.getSizeInBytes} bytes.")

  // Create Java object
  val javaObj = new TestData()

  // Create UnsafeRow (using your existing code)
  val schema = Array(StringType, IntegerType, StringType, StringType)
  val row = new SpecificInternalRow(schema)

  val testString = "abcd"
  val t1 = "l"
  val utf8String = UTF8String.fromString(testString)
  val utf8String2 = UTF8String.fromString(t1)
  row.update(0, utf8String)
  row.update(1, 20)
  row.update(2, utf8String2)
  row.setNullAt(3)

  val attrs = schema.zipWithIndex.map { case (dt, i) =>
    AttributeReference(s"c$i", dt)()
  }

  val converter = UnsafeProjection.create(attrs, attrs)
  val unsafeRow = converter.apply(row)

  // Get the actual binary size of UnsafeRow
  val binarySize = unsafeRow.getSizeInBytes

  // Detailed Java Object Memory Analysis
  println("Java Object Memory Analysis:")
  println("-" * 50)
  println("TestData object:")
  println(ClassLayout.parseInstance(javaObj).toPrintable())

  println("\nString 'abcd' object:")
  println(ClassLayout.parseInstance(javaObj.str1).toPrintable())

  println("\nString 'l' object:")
  println(ClassLayout.parseInstance(javaObj.str2).toPrintable())

  println("\nTotal Memory Footprint (including all referenced objects):")
  println(GraphLayout.parseInstance(javaObj).toFootprint())

  val totalJavaMemory = GraphLayout.parseInstance(javaObj).totalSize()
  println(s"\nTotal Java Object Memory: $totalJavaMemory bytes")

  // UnsafeRow Memory Analysis
  val unsafeRowSize = unsafeRow.getSizeInBytes
  println("\nUnsafeRow Memory Analysis:")
  println("-" * 50)
  println(s"Binary size: $unsafeRowSize bytes")

  // Memory Comparison
  println("\nMemory Comparison:")
  println("-" * 50)
  println(s"Java Object total memory: $totalJavaMemory bytes")
  println(s"UnsafeRow total memory: $unsafeRowSize bytes")
  println(s"Memory saved by UnsafeRow: ${totalJavaMemory - unsafeRowSize} bytes")
  println(s"Memory reduction: ${((totalJavaMemory - unsafeRowSize.toDouble) / totalJavaMemory * 100).round}%")

  // Detailed breakdown
  println("\nJava Object Memory Breakdown:")
  println("1. TestData object header")
  println("2. Four reference fields (str1, num, str2, str3)")
  println("3. String 'abcd' object:")
  println("   - String object header")
  println("   - char[] array header")
  println("   - Character data (8 bytes for 4 chars)")
  println("4. String 'l' object:")
  println("   - String object header")
  println("   - char[] array header")
  println("   - Character data (2 bytes for 1 char)")
  println("5. Integer field (4 bytes)")
  println("6. Alignment padding")

  println("\nUnsafeRow Memory Breakdown:")
  println("1. Null bitset (8 bytes)")
  println("2. Fixed-length fields")
  println("3. Variable-length field offsets")
  println("4. Actual string data")
  println("5. Minimum alignment padding")
}
