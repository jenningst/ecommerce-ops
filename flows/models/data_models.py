from dataclasses import dataclass

@dataclass
class ReviewData:
	"""Class for intermediate review data."""
	overall: float
	verified: bool
	review_time: str
	reviewer_id: str
	asin: str
	reviewer_name: str
	review_text: str
	tokenized_review_text: list[str]
	summary: str
	tokenized_summary: list[str]
	unix_review_time: int

@dataclass
class FeaturizedReviewData(ReviewData):
	""""""
	featurized_review_text: list[str]
	featurized_summary: list[str]