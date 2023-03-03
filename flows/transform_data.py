import json
import string

import spacy

from typing import List

from prefect import flow, task

from models import ReviewData, FeaturizedReviewData


@task(
    name="Tokenize review text and summary.",
    description="Task to tokenize the review for nlp preprocessing."
)
def tokenize_reviews(
    data: List[dict]
) -> List[ReviewData]:

    output = []
    for review in data:
        # remove punctuation, convert to lowercase, and split
        tokenized_text = review["reviewText"].translate(str.maketrans("", "", string.punctuation)).lower().split()
        tokenized_summary = review["summary"].translate(str.maketrans("", "", string.punctuation)).lower().split()

        # create an instance of the model and add to output
        tokenized_review = ReviewData(
            overall=review["overall"],
            verified=review["verified"],
            reviewTime=review["reviewTime"],
            reviewerID=review["reviewerID"],
            asin=review["asin"],
            reviewerName=review["reviewerName"],
            reviewText=review["reviewText"],
            tokenizedReviewText=tokenized_text,
            summary=review["summary"],
            tokenizedSummary=tokenized_summary,
            unixReviewTime=review["unixReviewTime"],
        )
        output.append(tokenized_review)
    return output


@task(
    name="Featurize review text and summary.",
    description="Task to conduct feature engineering on the review."
)
def featurize_review(
    data: List[ReviewData],
    nlp,
) -> List[FeaturizedReviewData]:
    output = []
    for review in data:
        doc = nlp(" ".join(review.tokenizedReviewText)) # TODO: Refactor
        review_lemma = [token.lemma_ for token in doc if not token.is_stop]

        doc = nlp(" ".join(review.tokenizedSummary)) # TODO: Refactor
        summary_lemma = [token.lemma_ for token in doc if not token.is_stop]

        # create updated model using the lemmatized text and add to output
        upgraded_review = FeaturizedReviewData(
            **review.dict(),
            featurizedReview=review_lemma,
            featurizedSummary=summary_lemma
        )
        output.append(upgraded_review)
    return output


# define flow and subflows
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
    nlp = spacy.load(name="en_core_web_sm")

    # load json
    with open("../data/processed/1073692800.json") as f_in:
        data = json.load(f_in)

    reviews = transform_data(
        nlp=nlp,
        data=data
    )