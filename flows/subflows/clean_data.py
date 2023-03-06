from typing import List

from prefect import flow, task

from models.data_models import (
    BaseReview, 
)


@task(
    name="Validate Reviews",
    description="Task to validate review data to support downstream transformation."
)
def validate_reviews(
    reviews: List[dict]
) -> List[BaseReview]:
    """Validate that review data has correct data. 

    Parameters
    ----------
    reviews : List[dict]
        List of raw json review instances

    Returns
    -------
    List[BaseReview]
        List of valid ReviewData models for each review instance
    """
    valid_review_features = {"summary", "verified", "reviewText", "summary", "unixReviewTime"}
    validated_reviews = []
    for review in reviews:
        if set(review.keys()) == valid_review_features:
            validated_reviews.append(BaseReview(
                    overall=float(review["overall"]),
                    verified=bool(review["verified"]),
                    review_text=str(review["reviewText"]),
                    summary=str(review["summary"]),
                    unix_review_time=int(review["unixReviewTime"]),
                )
            )
        return validated_reviews


@flow(
    name="Review Cleansing Flow",
    description="Subflow to handle data cleaning and validation prior to featurization"
)
def clean_data(
    reviews: List[dict]
) -> List[BaseReview]:
    """Cleanse and validation review data."""
    cleansed_reviews = validate_reviews(
        reviews=reviews
    )
    return cleansed_reviews
