from pydantic import BaseModel

class ReviewData(BaseModel):
	overall: float
	verified: bool
	reviewTime: str
	reviewerID: str
	asin: str
	reviewerName: str
	reviewText: str
	tokenizedReviewText: list[str]
	summary: str
	tokenizedSummary: list[str]
	unixReviewTime: int

class FeaturizedReviewData(ReviewData):
	featurizedReview: list[str]
	featurizedSummary: list[str]