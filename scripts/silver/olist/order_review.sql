select
  review_id as idReview,
  order_id as idOrder,
  review_score as vlScore,
  review_comment_title as descTitle,
  review_comment_message as descMessage,
  review_creation_date as dtCreation,
  review_answer_timestamp as dtAnswer

from bronze_olist.olist_order_reviews_dataset