WITH revenue_usd AS (
SELECT
 s.continent,
 SUM(p.price) AS revenue,
 SUM(CASE WHEN device = 'mobile' THEN p.price END) AS revenue_from_mobile,
 SUM(CASE WHEN device = 'desktop' THEN p.price END) AS revenue_from_desktop


FROM `data-analytics-mate.DA.product` p
  JOIN `data-analytics-mate.DA.order` o
  ON p.item_id = o.item_id
  JOIN `data-analytics-mate.DA.session_params` s
  ON o.ga_session_id = s.ga_session_id
GROUP BY s.continent
),

account AS (
  SELECT
   s.continent,
   COUNT(id) AS account_count,
   COUNT(CASE WHEN is_verified = 1 THEN id END) AS verified_account,
   COUNT(s.ga_session_id) AS session_cnt
  FROM `data-analytics-mate.DA.account` a
  LEFT JOIN `data-analytics-mate.DA.account_session` acs
  ON a.id = acs.account_id
  LEFT JOIN `data-analytics-mate.DA.session_params` s
  ON acs.ga_session_id = s.ga_session_id
  GROUP BY s.continent
),

session AS (
  SELECT
  continent,
  COUNT(*) AS session_cnt
  FROM data-analytics-mate.DA.session_params
  GROUP BY continent
)

SELECT
    r.continent,
    r.revenue,
    r.revenue_from_mobile,
    r.revenue_from_desktop,
    ROUND((r.revenue / SUM(r.revenue) OVER()) * 100, 2) AS percent_revenue_from_total,
    a.account_count,
    a.verified_account,
    ss.session_cnt
  FROM
    revenue_usd r
  LEFT JOIN
    account a
  ON
    r.continent = a.continent
  LEFT JOIN
    session ss
  ON
    r.continent = ss.continent
