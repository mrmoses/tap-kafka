import unittest
import tap_kafka

from confluent_kafka.serialization import SerializationError
from tap_kafka.serialization.json_with_no_schema import JSONSimpleDeserializer
from tap_kafka.serialization.protobuf import ProtobufDictDeserializer
from tap_kafka.serialization.protobuf import proto_to_message_type
from tap_kafka.serialization.protobuf import topic_name_to_protoc_output_name
from tap_kafka.errors import ProtobufCompilerException


class TestSerializers(unittest.TestCase):
    def test_generate_config_with_defaults(self):
        deserializer = JSONSimpleDeserializer()

        self.assertIsNone(deserializer(value=None, ctx=None))
        self.assertEqual(deserializer(value='{}', ctx=None), {})
        self.assertEqual(deserializer(value='{"abc": 123}', ctx=None), {'abc': 123})

        with self.assertRaises(SerializationError):
            deserializer(value='invalid-json', ctx=None)

    def test_protobuf_to_message_type(self):
        # Using invalid .proto schema should raise an exception
        with self.assertRaises(ProtobufCompilerException):
            proto_to_message_type('invalid-proto-schema', tap_kafka.DEFAULT_PROTO_CLASSES_DIR, 'test-topic')

        # Compile valid .proto to python class
        test_item_class = proto_to_message_type("""syntax = "proto3";
        message TestItem {
            string name = 1;
            int64 favourite_number = 2;
            string favourite_color = 3;
        }""", tap_kafka.DEFAULT_PROTO_CLASSES_DIR, 'test-topic')
        self.assertEqual(test_item_class.DESCRIPTOR.name, "TestItem")

        # Create an instance of the test_item_class
        test_item = test_item_class(name="test-name",
                                    favourite_number=99,
                                    favourite_color="pink")
        self.assertEqual(test_item.name, "test-name")
        self.assertEqual(test_item.favourite_number, 99)
        self.assertEqual(test_item.favourite_color, "pink")

    def test_protobuf_dict_deserializer(self):
        test_item_class = proto_to_message_type("""syntax = "proto3";
        message DictItem {
            string name = 1;
            int64 favourite_number = 2;
        }""", tap_kafka.DEFAULT_PROTO_CLASSES_DIR, 'test-dict-topic')
        deserializer = ProtobufDictDeserializer(test_item_class, {'use.deprecated.format': False})

        # Confluent wire format: magic byte, 4-byte schema id, message index [0], payload
        payload = test_item_class(name="test-name").SerializeToString()
        value = b'\x00' + (1).to_bytes(4, 'big') + b'\x00' + payload

        self.assertEqual(deserializer(value, None), {'name': 'test-name', 'favourite_number': '0'})

    def test_topic_name_to_protoc_output_name(self):
        """Should transform topic names to protoc equivalent output file names"""
        # Dash should be converted to underscores
        self.assertEqual(topic_name_to_protoc_output_name('test-topic'), 'test_topic')
        # Dots should be converted to underscores
        self.assertEqual(topic_name_to_protoc_output_name('test.topic'), 'test_topic')
        # Camelcase should remain
        self.assertEqual(topic_name_to_protoc_output_name('testTopic'), 'testTopic')
        # Mix of everything
        self.assertEqual(topic_name_to_protoc_output_name('test-topic.dummy.camelCase_foo'), 'test_topic_dummy_camelCase_foo')
