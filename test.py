# coding: utf-8
from api import api as zipkin_api
from time import time
from pprint import pprint

trace_id, span_id = zipkin_api.set_data(service_name="api", rpc_name="sql") #you should use this first, and then record
zipkin_api.record_event('ss')  # Note duration is in microseconds, as defined by Zipkin
zipkin_api.record_key_value('lc', "213")  # You can use string, int, long and bool values
pprint(zipkin_api.test_get_span())
zipkin_api.submit_span(time()*1000*1000, 123)
print trace_id.get_binary(), span_id.get_binary()

trace_id, c_span_id = zipkin_api.set_data(service_name="sql", trace_id=trace_id, rpc_name="do sql", parent_span_id=span_id)
zipkin_api.record_event('cr')  # Note duration is in microseconds, as defined by Zipkin
zipkin_api.record_key_value('lc', "123")  # You can use string, int, long and bool values
zipkin_api.submit_span(time()*1000*1000, 456)


trace_id, c1_span_id = zipkin_api.set_data(service_name="sql_1", trace_id=trace_id, rpc_name="do sql 1", parent_span_id=span_id)
zipkin_api.record_event('cr')  # Note duration is in microseconds, as defined by Zipkin
zipkin_api.record_key_value('lc', "123")  # You can use string, int, long and bool values
zipkin_api.submit_span(time()*1000*1000, 234)

trace_id, span_id = zipkin_api.set_data(service_name="sqlexec", trace_id=trace_id, rpc_name="exe sql", parent_span_id=c_span_id)
zipkin_api.record_event('cr')  # Note duration is in microseconds, as defined by Zipkin
zipkin_api.record_key_value('lc', "123")  # You can use string, int, long and bool values
zipkin_api.submit_span(time()*1000*1000, 1230)

trace_id, span_id = zipkin_api.set_data(service_name="sqlexec_1", trace_id=trace_id, rpc_name="exe sql", parent_span_id=c1_span_id)
zipkin_api.record_event('cr')  # Note duration is in microseconds, as defined by Zipkin
zipkin_api.record_key_value('lc', "123")  # You can use string, int, long and bool values
zipkin_api.submit_span(time()*1000*1000, 1230)
