import struct
import socket
import time

from data_store import default as default_store
from scribe_writer import ScribeWriter
from id_generator import default as default_gen
from zipkin_data import ZipkinData
from _thrift.zipkinCore.ttypes import Annotation, BinaryAnnotation, Endpoint, AnnotationType, Span


class ZipkinApi(object):
    def __init__(self, service_name=None, store=None, writer=None):
	port = 12345
        self.store = store or default_store
	self.store.clear()
        self.endpoint = Endpoint(
            ipv4=self._get_my_ip(),
            port=struct.unpack('h', struct.pack('H', port))[0],
            service_name=service_name
        )
        self.writer = writer

    def _set_not_none(self, key, func):
	if key is not None:
	    return key
	return func()

    def set_data(self, rpc_name="unknown", trace_id=None, span_id=None, parent_span_id=None, sampled=True):
	trace_id = self._set_not_none(trace_id, default_gen.generate_trace_id)
	span_id = self._set_not_none(span_id, default_gen.generate_span_id)
	data = ZipkinData(trace_id, span_id, parent_span_id, sampled, False)
	self.set_rpc_name(rpc_name)
	self.store.set(data)
	return (trace_id, span_id)

    def record_event(self, message):
        self.store.record(self._build_annotation(message))

    def record_key_value(self, key, value):
        self.store.record(self._build_binary_annotation(key, value))

    def set_rpc_name(self, name):
        self.store.set_rpc_name(name)

    def test_get_span(self):
	return self._build_span(123, 123)	
    
    def submit_span(self, timestamp_in_microseconds, duration_in_microseconds):
        self.writer.write(self._build_span(timestamp_in_microseconds, duration_in_microseconds))
        self.store.clear()

    def _get_my_ip(self):
        try:
            return self._ipv4_to_long(socket.gethostbyname(socket.gethostname()))
        except Exception:
            return None

    def _build_span(self, timestamp_in_microseconds, duration_in_microseconds):
        zipkin_data = self.store.get()
        return Span(
            id=zipkin_data.span_id.get_binary(),
            trace_id=zipkin_data.trace_id.get_binary(),
            parent_id=zipkin_data.parent_span_id.get_binary() if zipkin_data.parent_span_id is not None else None,
            name=self.store.get_rpc_name(),
            annotations=self.store.get_annotations(),
            binary_annotations=self.store.get_binary_annotations(),
            timestamp=timestamp_in_microseconds,
            duration=duration_in_microseconds
        )

    def _build_annotation(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return Annotation(time.time() * 1000 * 1000, str(value), self.endpoint)

    def _build_binary_annotation(self, key, value):
        annotation_type = self._binary_annotation_type(value)
        formatted_value = self._format_binary_annotation_value(value, annotation_type)
        return BinaryAnnotation(key, formatted_value, annotation_type, self.endpoint)

    @classmethod
    def _binary_annotation_type(cls, value):
        if isinstance(value, str) or isinstance(value, unicode):
            return AnnotationType.STRING
        if isinstance(value, float):
            return AnnotationType.DOUBLE
        if isinstance(value, bool):
            return AnnotationType.BOOL
        if isinstance(value, int) or isinstance(value, long):
            # TODO: make this more granular to preserve network bytes
            return AnnotationType.I64

    @classmethod
    def _format_binary_annotation_value(cls, value, type):
        number_formats = {
            AnnotationType.I16: 'h',
            AnnotationType.I32: 'i',
            AnnotationType.I64: 'q',
            AnnotationType.DOUBLE: 'd'
        }
        if type == AnnotationType.STRING:
            if isinstance(value, unicode):
                return value.encode('utf-8')
            return str(value)
        if type == AnnotationType.BOOL:
            if value:
                return '1'
            else:
                return '0'
        if type in number_formats:
            return struct.pack('!' + number_formats[type], value)
        return 'zipkin_cat failed to serialize type %s value %s' % (type, value)

    @staticmethod
    def _ipv4_to_long(ip):
        packed_ip = socket.inet_aton(ip)
        return struct.unpack("!i", packed_ip)[0]

#the args of the ScribeWriter is the same as your scribe conf, please modify them if your conf is different
api = ZipkinApi(service_name="test", store=default_store, writer=ScribeWriter(host="127.0.0.1", port=8888))
