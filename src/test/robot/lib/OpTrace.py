import csv


class CreateTraceRecord:
	def __init__(self):
		pass
	time_start_micros = 0
	duration_micros = 0


def validate_create_read_pipeline_op_trace_log_file(file_name, err_count_limit, read_count_limit):
	err_count = 0
	read_count = 0
	create_trace_recs = dict()
	with open(file_name, "rb") as op_trace_file:
		reader = csv.reader(op_trace_file)
		for row in reader:
			status_code = row[3]
			if status_code != 4:
				err_count += 1
				assert err_count < err_count_limit
				continue
			item_path = row[1]
			op_type_code = row[2]
			time_start_micros = row[4]
			if op_type_code == 1:
				create_trace_rec = CreateTraceRecord()
				create_trace_rec.time_start_micros = time_start_micros
				duration_micros = row[5]
				create_trace_rec.duration_micros = duration_micros
				create_trace_recs[item_path] = create_trace_rec
			elif op_type_code == 2:
				create_trace_rec = create_trace_recs.pop(item_path, None)
				assert create_trace_rec is not None
				read_latency_micros = row[6]
				e2e_latency_micros = time_start_micros + read_latency_micros - create_trace_rec.time_start_micros \
					- create_trace_rec.duration_micros
				assert e2e_latency_micros > 0
				read_count += 1
		assert read_count + err_count == read_count_limit
