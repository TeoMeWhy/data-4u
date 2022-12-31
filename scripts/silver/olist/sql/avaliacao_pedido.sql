select
  review_id as idAvaliacao,
  order_id as idPedido,
  int(review_score) as vlNota,
  review_comment_title as descTituloComentario,
  review_comment_message as descMensagemComentario,
  to_timestamp(review_creation_date) as dtAvaliacao,
  to_timestamp(review_answer_timestamp) as dtResposta

from bronze_olist.order_reviews