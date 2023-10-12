package dev.michaelpetri.kafka.connect.uuid.smt

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.nio.ByteBuffer
import java.util.*

abstract class UUIDTransformer<R : ConnectRecord<R>?> : Transformation<R> {

    private interface ConfigurationNames {
        companion object {
            const val FIELD_NAME = "field_name"
            const val TARGET_TYPE = "target_type"
        }
    }

    private var fieldName: String? = null
    private lateinit var targetType: FieldType
    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEFINITION, props)
        fieldName = config.getString(ConfigurationNames.FIELD_NAME)
        targetType = config.getFieldType(ConfigurationNames.TARGET_TYPE)
    }

    override fun apply(record: R): R = if (null === fieldName) {
        applyDirect(record)
    } else if (operatingSchema(record) == null) {
        applySchemaless(record)
    } else {
        applyWithSchema(record)
    }

    private fun applyDirect(record: R): R {
        val value = operatingValue(record)

        if (null === value) {
            return record
        }

        return newRecord(
            record,
            null,
            convert(value)
        )
    }

    @Throws(RuntimeException::class)
    private fun applySchemaless(record: R): R {
        val message = Requirements.requireMap(operatingValue(record), PURPOSE)
        val value = message[fieldName]

        if (null === value) {
            return record
        }

        val updatedMessage = message.toMutableMap()
        updatedMessage[fieldName] = convert(value)

        return newRecord(
            record,
            null,
            updatedMessage
        )
    }

    private fun applyWithSchema(record: R): R {
        val message = Requirements.requireStruct(operatingValue(record), PURPOSE)

        val value = message[fieldName]

        if (null === value) {
            return record
        }

        val updatedMessage = message.withField(
            fieldName!!,
            convert(value),
            when (targetType) {
                FieldType.STRING -> Schema.STRING_SCHEMA
                FieldType.BINARY -> Schema.BYTES_SCHEMA
            }
        )

        return newRecord(
            record,
            updatedMessage.schema(),
            updatedMessage
        )
    }

    private fun convert(value: Any) = denormalize(
        normalize(value)
    )

    private fun normalize(value: Any): UUID = when (value) {
        is String -> UUID.fromString(value)
        is ByteBuffer -> value.toUUID()
        else -> throw RuntimeException("Failed to normalize value: Unknown source type ${value::class}")
    }

    private fun denormalize(value: UUID) = when (targetType) {
        FieldType.STRING -> value.toString()
        FieldType.BINARY -> value.toByteBuffer()
    }

    override fun config(): ConfigDef = CONFIG_DEFINITION

    override fun close() {}
    protected abstract fun operatingSchema(record: R): Schema?
    protected abstract fun operatingValue(record: R): Any?
    protected abstract fun newRecord(record: R, updatedSchema: Schema?, updatedValue: Any?): R
    class Key<R : ConnectRecord<R>> : UUIDTransformer<R>() {
        override fun operatingSchema(record: R): Schema? {
            return record.keySchema()
        }

        override fun operatingValue(record: R): Any {
            return record.key()
        }

        override fun newRecord(record: R, updatedSchema: Schema?, updatedValue: Any?): R {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedSchema,
                updatedValue,
                record.valueSchema(),
                record.value(),
                record.timestamp()
            )
        }
    }

    class Value<R : ConnectRecord<R>> : UUIDTransformer<R>() {
        override fun operatingSchema(record: R): Schema? {
            return record.valueSchema()
        }

        override fun operatingValue(record: R): Any? {
            return record.value()
        }

        override fun newRecord(record: R, updatedSchema: Schema?, updatedValue: Any?): R {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
            )
        }
    }

    enum class FieldType {
        STRING,
        BINARY;
        companion object {
            fun fromString(value: String): FieldType = values().first {
                it.toString().lowercase() == value
            }
        }
    }

    companion object {
        private const val PURPOSE = "Converts uuids between string and bytes."

        private val CONFIG_DEFINITION = ConfigDef()
            .define(
                ConfigurationNames.FIELD_NAME,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                "Name of the source field"
            )
            .define(
                ConfigurationNames.TARGET_TYPE,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                "Target type of the field, possible values: string, byte"
            )
    }
}

private fun Struct.withField(fieldName: String, value: Any?, schema: Schema): Struct {
    val updatedSchema = schema().withField(
        fieldName,
        schema
    )
    val updatedMessage = Struct(updatedSchema)
    for (field in schema().fields()) {
        if (fieldName != field.name()) {
            updatedMessage.put(field.name(), get(field))
        }
    }

    updatedMessage.put(
        fieldName,
        value
    )

    return updatedMessage
}

private fun Schema.withField(name: String, schema: Schema): Schema = fields()
    .fold(
        SchemaBuilder.struct()
    ) { acc, it ->
        if (it.name() == name) acc else acc.field(it.name(), it.schema())
    }
    .field(name, schema)
    .build()

private fun SimpleConfig.getFieldType(key: String): UUIDTransformer.FieldType = UUIDTransformer.FieldType.fromString(
    getString(key)
)

fun UUID.toByteBuffer(): ByteBuffer = let {
    val bb = ByteBuffer
        .allocate(16)
        .putLong(it.mostSignificantBits)
        .putLong(it.leastSignificantBits)

    bb.flip()

    bb
}

fun ByteBuffer.toUUID(): UUID = UUID(getLong(), getLong())
