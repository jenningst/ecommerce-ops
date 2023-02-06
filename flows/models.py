from pydantic import BaseModel

class ReviewData(BaseModel):
	overall: float
	verified: bool
	reviewTime: str
	reviewerId: str
	asin: str
	reviewerName: str
	reviewText: str
	summary: str
	unixReviewTime: int
