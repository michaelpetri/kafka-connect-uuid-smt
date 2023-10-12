# Kafka Connect UUID SMT

This package contains a single message transformer for kafka connect to convert uuids between string and binary.

# Usage

Convert a binary uuid field to a string:

```json
{
  "config": {
    "transforms": "uuid_to_string,uuid_to_binary",
    "transforms.uuid_to_string.type": "dev.michaelpetri.kafka.connect.uuid.smt.UUIDTransformer$Value",
    "transforms.uuid_to_string.field_name": "your_binary_uuid",
    "transforms.uuid_to_string.target_type": "string"
  }
}
```

Convert a string uuid field to a binary:

```json
{
  "config": {
    "transforms": "uuid_to_string,uuid_to_binary",
    "transforms.uuid_to_string.type": "dev.michaelpetri.kafka.connect.uuid.smt.UUIDTransformer$Value",
    "transforms.uuid_to_string.field_name": "your_string_field",
    "transforms.uuid_to_string.target_type": "binary"
  }
}
```