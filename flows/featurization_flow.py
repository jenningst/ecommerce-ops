import json
import os
import spacy
import time

from typing import List

from dotenv import load_dotenv
from prefect import flow

from subflows import clean_data, transform_data

load_dotenv(override=True)


# load nlp pipeline
nlp_pipeline = spacy.load(name="en_core_web_sm") # TODO: don't include tokenizer


@flow(name="Review featurization flow")
def featurization_pipeline(
    reviews: List[dict],
    nlp_pipeline
):
    """Execute subflows for data preparation for feature store."""
    cleansed_reviews = clean_data.clean_data(
        reviews=reviews
    )
    transform_data.transform_data(
        reviews=cleansed_reviews,
        nlp_pipeline=nlp_pipeline
    )


if __name__ == "__main__":
    # load the local output path for all json instances and batch size
    processed_input_path = os.getenv("PROCESSED_OUTPUT_PATH")
    batch_size = int(os.getenv("PROCESSING_BATCH_SIZE"))

    # batch process the queue
    review_batch = []
    reviews_to_process = os.listdir(processed_input_path)
    for i in range(0, len(reviews_to_process) + 1, batch_size):
        flow_start = time.time()
        for review in reviews_to_process[i:i+batch_size]:
            if review.endswith(".json"):
                with open(os.path.join(processed_input_path, review)) as json_in:
                    review_batch.extend(json.load(json_in))

        # execute the flow for each batch
        featurization_pipeline(
            reviews=review_batch,
            nlp_pipeline=nlp_pipeline
        )
        flow_end = time.time()
        print(f"Featurization flow for batch {i/batch_size} took {flow_end-flow_start} seconds")