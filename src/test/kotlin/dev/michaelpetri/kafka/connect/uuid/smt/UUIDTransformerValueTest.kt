package dev.michaelpetri.kafka.connect.uuid.smt

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import java.nio.ByteBuffer
import java.util.UUID
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UUIDTransformerValueTest {
    private val uuidString = "c97fe7c5-cce5-49f2-96ec-71d215c41273"
    private val uuidObject = UUID.fromString(uuidString)
    private val uuidByteBuffer = uuidObject.toByteBuffer()

    private val valueTransformer: UUIDTransformer<SourceRecord> = UUIDTransformer.Value()

    @Test
    fun `uuid value gets converted from String to String (unstructured)`() {
        val record = makeUnstructuredRecord(uuidString)

        valueTransformer.configure(
            mapOf(
                "target_type" to "string"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            uuidString,
            transformedRecord.value()
        )
    }

    @Test
    fun `uuid value gets converted from String to ByteBuffer (unstructured)`() {
        val record = makeUnstructuredRecord(uuidString)

        valueTransformer.configure(
            mapOf(
                "target_type" to "binary"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertTrue {
            uuidByteBuffer.equals(
                transformedRecord.value()
            )
        }
    }

    @Test
    fun `uuid value gets converted from ByteBuffer to String (unstructured)`() {
        val record = makeUnstructuredRecord(uuidByteBuffer)

        valueTransformer.configure(
            mapOf(
                "target_type" to "string"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            uuidString,
            transformedRecord.value()
        )
    }

    @Test
    fun `uuid value gets converted from String to String (structured, schemaless)`() {
        val record = makeStructuredRecordWithoutSchema(
            "uuid" to uuidString
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "string"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            uuidString,
            (transformedRecord.value() as Map<*, *>)["uuid"]
        )
    }

    @Test
    fun `uuid value gets converted from String to ByteBuffer (structured, schemaless)`() {
        val record = makeStructuredRecordWithoutSchema(
            "uuid" to uuidString
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "binary"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertTrue {
            uuidByteBuffer.equals(
                (transformedRecord.value() as Map<*, *>)["uuid"]
            )
        }
    }

    @Test
    fun `uuid value gets converted from ByteBuffer to String (structured, schemaless)`() {
        val record = makeStructuredRecordWithoutSchema(
            "uuid" to uuidByteBuffer
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "string"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            uuidString,
            (transformedRecord.value() as Map<*, *>)["uuid"]
        )
    }

    @Test
    fun `uuid value gets converted from String to String (structured, with schema)`() {
        val record = makeStructuredRecordWithSchema(
            "uuid" to uuidString
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "string"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            Schema.STRING_SCHEMA,
            transformedRecord.valueSchema().field("uuid").schema()
        )

        assertEquals(
            uuidString,
            (transformedRecord.value() as Struct)["uuid"]
        )
    }

    @Test
    fun `uuid value gets converted from String to ByteBuffer (structured, with schema)`() {
        val record = makeStructuredRecordWithSchema(
            "uuid" to uuidString
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "binary"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            Schema.BYTES_SCHEMA,
            transformedRecord.valueSchema().field("uuid").schema()
        )

        assertTrue {
            uuidByteBuffer.equals(
                (transformedRecord.value() as Struct)["uuid"]
            )
        }
    }

    @Test
    fun `uuid value gets converted from ByteBuffer to String (structured, with schema)`() {
        val record = makeStructuredRecordWithSchema(
            "uuid" to uuidByteBuffer
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "string"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            Schema.STRING_SCHEMA,
            transformedRecord.valueSchema().field("uuid").schema()
        )

        assertEquals(
            uuidString,
            (transformedRecord.value() as Struct)["uuid"]
        )
    }

    @Test
    fun `uuid value gets converted from ByteBuffer to ByteBuffer (structured, with schema)`() {
        val record = makeStructuredRecordWithSchema(
            "uuid" to uuidByteBuffer
        )

        valueTransformer.configure(
            mapOf(
                "field_name" to "uuid",
                "target_type" to "binary"
            )
        )

        val transformedRecord = valueTransformer.apply(record)

        assertEquals(
            Schema.BYTES_SCHEMA,
            transformedRecord.valueSchema().field("uuid").schema()
        )

        assertTrue {
            uuidByteBuffer.equals(
                (transformedRecord.value() as Struct)["uuid"]
            )
        }
    }

    private fun makeUnstructuredRecord(value: Any?): SourceRecord = SourceRecord(
        null,
        null,
        "test",
        0,
        null,
        value,
        null,
        value
    )

    private fun makeStructuredRecordWithoutSchema(vararg pairs: Pair<String, Any?>): SourceRecord = SourceRecord(
        null,
        null,
        "test",
        0,
        null,
        mapOf(*pairs),
        null,
        mapOf(*pairs)
    )

    private fun makeStructuredRecordWithSchema(vararg pairs: Pair<String, Any?>): SourceRecord {
        val builder = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")

        pairs.forEach {
            builder.field(
                it.first,
                when (it.second) {
                    is String -> Schema.STRING_SCHEMA
                    is ByteBuffer -> Schema.BYTES_SCHEMA
                    else -> throw RuntimeException("Unknown field value type: ${it::class.java}")
                }
            )
        }

        val schema = builder.build()
        val values = pairs.fold(Struct(schema)) { struct, pair ->
            struct.put(
                pair.first,
                when (pair.second) {
                    is String -> pair.second
                    is ByteBuffer -> pair.second
                    else -> throw RuntimeException("Unknown field value type: ${pair::class.java}")
                }
            )
        }

        return SourceRecord(
            null,
            null,
            "test",
            0,
            schema,
            values,
            schema,
            values
        )
    }

    @AfterTest
    fun tearDown() = valueTransformer.close()
}
