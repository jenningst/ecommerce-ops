from dataclasses import dataclass

@dataclass
class BaseReview:
	"""Class for raw review data."""
	overall: float
	verified: bool
	review_text: str
	summary: str
	unix_review_time: int


	def __post_init__(self):
		if self.review_text == "" or self.review_text == None:
			raise ValueError(f"Review text cannot be None or empty; got {self.review_text}")
		if self.unix_review_time == None:
			raise ValueError(f"Unix review time cannot be None; got {self.unix_review_time}")


@dataclass
class IntermediateReview(BaseReview):
	"""Class for intermediate, tokenized review data."""
	tokenized_review_text: list[str]
	tokenized_summary: list[str]


@dataclass
class FeaturizedReviewData(IntermediateReview):
	"""Class for featurized review data."""
	featurized_review_text: list[str]
	featurized_summary: list[str]