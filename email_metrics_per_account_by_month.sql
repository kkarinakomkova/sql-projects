SELECT DISTINCT
   sent_month,
   id_account,
   (sent_msg / sent_msg_total) * 100 AS sent_msg_percent_from_this_month,
   first_sent_date,
   last_sent_date

FROM (

SELECT
  DATE_TRUNC(DATE_ADD(se.date, INTERVAL s.sent_date DAY), MONTH) AS sent_month,
  s.id_account,
  COUNT(id_message) OVER(PARTITION BY DATE_TRUNC(DATE_ADD(se.date, INTERVAL s.sent_date DAY), MONTH), s.id_account) AS sent_msg,
  COUNT(id_message) OVER(PARTITION BY DATE_TRUNC(DATE_ADD(se.date, INTERVAL s.sent_date DAY), MONTH)) AS sent_msg_total,
  MIN(DATE_ADD(se.date, INTERVAL s.sent_date DAY)) OVER(PARTITION BY s.id_account, DATE_TRUNC(DATE_ADD(se.date, INTERVAL s.sent_date DAY), MONTH)) AS first_sent_date,
  MAX(DATE_ADD(se.date, INTERVAL s.sent_date DAY)) OVER(PARTITION BY s.id_account, DATE_TRUNC(DATE_ADD(se.date, INTERVAL s.sent_date DAY), MONTH)) AS last_sent_date

FROM data-analytics-mate.DA.email_sent s
  JOIN data-analytics-mate.DA.account_session a
  ON s.id_account = a.account_id
  JOIN data-analytics-mate.DA.session se
  ON a.ga_session_id = se.ga_session_id
) data_2

ORDER BY sent_month
