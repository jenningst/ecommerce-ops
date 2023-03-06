import gzip
import json
import os

from collections import defaultdict
from typing import Dict, List

from dotenv import load_dotenv
from prefect import flow, task

from models.data_models import ReviewData

load_dotenv(override=True)


@task(
	name="Aggregate data by timestamp",
	description="Aggregates reviews by their unix timestamp."
)
def aggregate_data_by_timestamp(source_input_path: str) -> Dict[str, List[ReviewData]]:
	"""
	Parameters
	----------
	source_input_path : str
		Path to the input source file

	Returns
	-------
	output : Dict[str, List[ReviewData]]]
		Dictionary with timestamp as keys and list of review data as values
	"""
	output = defaultdict(list)
	with gzip.open(source_input_path, "r") as f_in:
		for line in f_in.readlines():
			element = json.loads(line)
			output[element["unixReviewTime"]].append(element)
	return output


@task(
	name="Write data by timestamp",
	description="Writes individual json output for each timestamp."
)
def write_data_by_timestamp(data: Dict[str, List[ReviewData]], processed_output_path: str) -> None:
	"""
	Parameters
	----------
	data : Dict[str, List[ReviewData]]
		Timestamp-aggregated json from prior task
	processed_output_path : str
		Path to write the output data
	"""
	for key, value in data.items():
		file_path = os.path.join(processed_output_path, f"{key}.json")
		with open(file_path, "w") as f_out:
			f_out.write(json.dumps(value))


@flow(
	name="Data splitting flow",
	description="Flow to handle data pre-processing."
)
def split_data(source_input_path: str, processed_output_path: str) -> None:
	aggregated_data = aggregate_data_by_timestamp(
		source_input_path=source_input_path
	)
	write_data_by_timestamp(
		data=aggregated_data,
		processed_output_path=processed_output_path
	)


if __name__ == "__main__":
	source_input_path = os.getenv("SOURCE_INPUT_PATH")
	processed_output_path = os.getenv("PROCESSED_OUTPUT_PATH")

	split_data(
		source_input_path=source_input_path,
		processed_output_path=processed_output_path
	)