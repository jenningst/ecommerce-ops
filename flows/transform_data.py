import json
import string

import spacy

from typing import List

from prefect import flow, task

from models.data_models import ReviewData, FeaturizedReviewData


@task(
    name="Tokenize review text and summary.",
    description="Task to tokenize the review for nlp preprocessing."
)
def tokenize_reviews(data: List[dict]) -> List[ReviewData]:
    """Tokenize and cleanse review text.

    Parameters
    ----------
    data : List[dict]
        Raw json review samples

    Returns
    -------
    List[ReviewData]
        Tokenized and pre-cleansed review texts
    """
    reviews = []
    for review in data:
        # remove punctuation, convert to lowercase, and split
        tokenized_text = review["reviewText"].translate(str.maketrans("", "", string.punctuation)).lower().split()
        tokenized_summary = review["summary"].translate(str.maketrans("", "", string.punctuation)).lower().split()

        # create an instance of the model and add to output
        tokenized_review = ReviewData(
            overall=review["overall"],
            verified=review["verified"],
            review_time=review["reviewTime"],
            reviewer_id=review["reviewerID"],
            asin=review["asin"],
            reviewer_name=review["reviewerName"],
            review_text=review["reviewText"],
            tokenized_review_text=tokenized_text,
            summary=review["summary"],
            tokenized_summary=tokenized_summary,
            unix_review_time=review["unixReviewTime"],
        )
        reviews.append(tokenized_review)
    return reviews


@task(
    name="Featurize review text and summary.",
    description="Task to conduct feature engineering on the review."
)
def featurize_review(data: List[ReviewData], nlp) -> List[FeaturizedReviewData]:
    """Lemmatize review text.

    Parameters
    ----------
    data : List[ReviewData]
        Review data with tokenized review data
    nlp : _type_
        spaCy nlp model for running a lemmatization pipeline

    Returns
    -------
    List[FeaturizedReviewData]
        Featurized review data
    """
    output = []
    for review in data:
        doc = nlp(" ".join(review.tokenized_review_text))
        review_lemma = [token.lemma_ for token in doc if not token.is_stop]

        doc = nlp(" ".join(review.tokenized_summary))
        summary_lemma = [token.lemma_ for token in doc if not token.is_stop]

        # create updated model using the lemmatized text and add to output
        featurized_review = FeaturizedReviewData(
            **review.__dict__,
            featurized_review_text=review_lemma,
            featurized_summary=summary_lemma
        )
        output.append(featurized_review)
    return output


@flow(
    name="Transform data",
    description="Flow to handle data transformation."
)
def transform_data(
    data: List[dict],
    nlp
):
    # tokenize review text and summary
    tokenized_reviews = tokenize_reviews(
        data=data
    )
    # featurized review text
    reviews = featurize_review(
        data=tokenized_reviews,
        nlp=nlp,
    )
    return reviews

if __name__ == "__main__":
    # load the spacy model and create a pipeline for nlp processing
    nlp = spacy.load(name="en_core_web_sm") # TODO: don't include tokenizer

    # load json
    with open("../data/processed/1073692800.json") as f_in:
        data = json.load(f_in)

    reviews = transform_data(
        nlp=nlp,
        data=data
    )