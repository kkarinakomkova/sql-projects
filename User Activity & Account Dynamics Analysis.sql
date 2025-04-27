-- calculate metrics for accounts
WITH account_metrics AS (
SELECT
  s.date,
  sp.country,
  ac.send_interval,
  ac.is_verified,
  ac.is_unsubscribed,
  COUNT(DISTINCT ac.id) AS account_cnt

  FROM `data-analytics-mate.DA.account` ac
  JOIN `data-analytics-mate.DA.account_session` acs ON ac.id = acs.account_id
  JOIN `data-analytics-mate.DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
  JOIN `data-analytics-mate.DA.session` s ON sp.ga_session_id = s.ga_session_id

  GROUP BY
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed
),
-- calculate metrics for emails
email_metrics AS (
  SELECT
  DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS sent_date,
  sp.country,
  ac.send_interval,
  ac.is_verified,
  ac.is_unsubscribed,
  COUNT(DISTINCT es.id_message) AS sent_msg,
  COUNT(DISTINCT eo.id_message) AS open_msg,
  COUNT(DISTINCT ev.id_message) AS visit_msg

  FROM `data-analytics-mate.DA.email_sent` es
  LEFT JOIN `data-analytics-mate.DA.email_open` eo
  ON es.id_message = eo.id_message
  LEFT JOIN `data-analytics-mate.DA.email_visit` ev
  ON es.id_message = ev.id_message
  JOIN `data-analytics-mate.DA.account` ac
  ON es.id_account = ac.id
  JOIN `data-analytics-mate.DA.account_session` acs ON ac.id = acs.account_id
  JOIN `data-analytics-mate.DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
  JOIN `data-analytics-mate.DA.session` s ON sp.ga_session_id = s.ga_session_id

  GROUP BY
  DATE_ADD(s.date, INTERVAL es.sent_date DAY),
  sp.country,
  ac.send_interval,
  ac.is_verified,
  ac.is_unsubscribed
),

final_union AS (
 SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  0 AS sent_msg,
  0 AS open_msg,
  0 AS visit_msg

  FROM account_metrics

UNION ALL

  SELECT
  sent_date AS date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  0 AS account_cnt,
  sent_msg,
  open_msg,
  visit_msg

  FROM email_metrics
),


-- join final_union into one table
final_data AS (
  SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  SUM(account_cnt) AS account_cnt,
  SUM(sent_msg) AS sent_msg,
  SUM(open_msg) AS open_msg,
  SUM(visit_msg) AS visit_msg

  FROM final_union

  GROUP BY
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed
),

-- calculate totals
total_country AS (
  SELECT
  *,
  SUM(account_cnt) OVER(PARTITION BY country) AS total_country_account_cnt,
  SUM(sent_msg) OVER(PARTITION BY country) AS total_country_sent_cnt

  FROM final_data
),

-- calculate rank
rank_country AS (
  SELECT
  *,
  DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
  DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
 
  FROM total_country
)

SELECT
*
FROM rank_country

WHERE rank_total_country_account_cnt <=10
      OR
      rank_total_country_sent_cnt <=10


ORDER BY rank_total_country_account_cnt, rank_total_country_sent_cnt
