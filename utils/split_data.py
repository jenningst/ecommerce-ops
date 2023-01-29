import gzip
import json

from collections import defaultdict


def split_data_by_unix_timestamp(
	input_file_path: str, 
	intermediate_output_path: str,
	processed_output_path: str,
) -> None:
	"""Decompress raw data from gzip format, split it based on timestamp, and
	store locally.

	Arguments:
		input_path (str) -- Input path to the raw data.
		intermediate_output_path (str) -- Output path to the intermediate data.
		processed_output_path (str) -- Output path for each of the split, processed data.
	"""
	output = defaultdict(list)

	# build a dictionary of json objects grouped by the unixtimestamp key value
	with gzip.open(input_file_path, "r") as f_in:
		with open(f"{intermediate_output_path}{input_file_path.split('/')[-1][:-3].lower()}", "w") as f_out:
			for line in f_in.readlines():
				element = json.loads(line)
				output[element["unixReviewTime"]].append(element)
			# write the entire itermediate json output
			f_out.write(json.dumps(output))

	# write grouped data (by unixtimestamp) to individual files
	for key, value in output.items():
		with open(f"{processed_output_path}{input_file_path.split('/')[-1][:-3].lower()}_{key}.json", "w") as f_out:
			f_out.write(json.dumps(value))


if __name__ == "__main__":
	split_data_by_unix_timestamp(
		input_file_path="../data/raw/Prime_Pantry.json.gz",
		intermediate_output_path="../data/intermediate/",
		processed_output_path="../data/processed/"
	)
