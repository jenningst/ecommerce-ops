import string

import spacy

from typing import List

from prefect import flow, task

from models.data_models import (
    BaseReview, 
    IntermediateReview, 
    FeaturizedReviewData
)


@task(
    name="Tokenize Reviews",
    description="Task to tokenize the review for nlp preprocessing."
)
def tokenize_reviews(
    reviews: List[BaseReview]
) -> List[IntermediateReview]:
    """Tokenizes and cleanses text from a list of review data.

    Parameters
    ----------
    data : List[BaseReview]
        List of raw json review instances

    Returns
    -------
    List[IntermediateReview]
        List of tokenized and cleansed review data
    """
    tokenized_reviews = []
    for review in reviews:
        # remove punctuation, convert to lowercase, and split
        tokenized_text = review.review_text.translate(str.maketrans("", "", string.punctuation)).lower().split()
        tokenized_summary = review.summary.translate(str.maketrans("", "", string.punctuation)).lower().split()
        tokenized_reviews.append(IntermediateReview(
                **review.__dict__,
                tokenized_review_text=tokenized_text,
                tokenized_summary=tokenized_summary
            )
        )
    return tokenized_reviews


@task(
    name="Featurize Reviews",
    description="Task to conduct feature engineering on the review data."
)
def featurize_reviews(
    reviews: List[IntermediateReview],
    nlp_pipeline
) -> List[FeaturizedReviewData]:
    """Lemmatizes review text from a list of review data.

    Parameters
    ----------
    data : List[ReviewData]
        List of tokenized review data

    Returns
    -------
    List[FeaturizedReviewData]
        List of featurized review data
    """
    featurized_reviews = []
    for review in reviews:
        # extract the lemma for review text and summary
        review_doc = nlp_pipeline(" ".join(review.tokenized_review_text))
        review_lemma = [token.lemma_ for token in review_doc if not token.is_stop]

        summary_doc = nlp_pipeline(" ".join(review.tokenized_summary))
        summary_lemma = [token.lemma_ for token in summary_doc if not token.is_stop]
        
        # create updated model using the lemmatized text and add to output
        featurized_reviews.append(FeaturizedReviewData(
                **review.__dict__,
                featurized_review_text=review_lemma,
                featurized_summary=summary_lemma
            )
        )
    return featurized_reviews


@task(
    name="Load Reviews",
    description="Task to load review data into database."
)
def write_to_db(
    reviews: List[FeaturizedReviewData]
):
    # connect to db
    # load reviews into db
    for review in reviews:
        pass



@flow(
    name="Review Featurization Flow",
    description="Subflow to handle data pre-processing, featurization, and loading (into db)"
)
def transform_data(
    reviews: List[BaseReview],
    nlp_pipeline
) -> None:
    """Tokenize, featurize, and write review data."""
    tokenized_reviews = tokenize_reviews(
        reviews=reviews
    )
    # featurized review text
    featurized_reviews = featurize_reviews(
        reviews=tokenized_reviews,
        nlp_pipeline=nlp_pipeline
    )
    # write review to database
    write_to_db(
        reviews=featurized_reviews
    )