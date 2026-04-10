"""
update_data.py — queries BigQuery and patches RAW_DATA in index.html
Runs inside GitHub Actions; credentials come from GCP_SA_KEY secret.
"""
import json
import os
import re

from google.cloud import bigquery
from google.oauth2 import service_account

# ── Credentials ─────────────────────────────────────────────────────────────
sa_info = json.loads(os.environ["GCP_SA_KEY"])
credentials = service_account.Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/bigquery"],
)
client = bigquery.Client(credentials=credentials, project="coderhouse-data")

# ── Query ────────────────────────────────────────────────────────────────────
QUERY = """
WITH base AS (
  SELECT
    FORMAT_DATETIME('%Y-%m', created_at) AS mes,
    CASE
      WHEN country_code = 'AR' THEN 'AR'
      WHEN country_code = 'UY' THEN 'UY'
      WHEN country_code IN ('CL','CO','MX','PE','BR') THEN 'Latam'
      ELSE 'Others'
    END AS cluster,
    amount_usd AS revenue,
    CASE
      WHEN coupon IS NULL THEN 'Sin cupón'
      WHEN subchannel = 'Partnerships' THEN 'Partnerships'
      WHEN subchannel = 'Influencers' THEN 'Influencers'
      WHEN subchannel = 'Plataforma' AND coupon IS NOT NULL THEN 'Plataforma'
      WHEN coupon LIKE 'REF.%' OR coupon LIKE 'REF_%' THEN 'Referidos'
      WHEN coupon LIKE 'CODER.%' THEN 'Referidos'
      WHEN subchannel = 'Performance' AND coupon IS NOT NULL THEN 'Performance'
      WHEN subchannel = 'Social Media' AND coupon IS NOT NULL THEN 'Social Media'
      ELSE 'Directo/Orgánico'
    END AS cupon_parent,
    CASE
      WHEN coupon IS NULL THEN NULL
      WHEN subchannel IN ('Partnerships','Influencers') THEN CONCAT(subchannel, ' / ', COALESCE(coupon, ''))
      ELSE NULL
    END AS cupon_detail,
    CASE
      WHEN source = 'adwords' AND medium = 'ppc' THEN 'Google Ads'
      WHEN source = 'google' AND medium = 'cpc' THEN 'Google Ads'
      WHEN source LIKE 'facebook%' AND medium IN ('cpc', 'paid') THEN 'Meta Ads'
      WHEN source = 'tiktok' THEN 'TikTok Ads'
      WHEN source = 'whatsapp' THEN 'Whatsapp'
      WHEN source = 'ig' OR source LIKE 'ig%' THEN 'Social Media'
      WHEN medium = 'alianzas' OR medium = 'partnerships' THEN 'Partnerships (UTM)'
      WHEN source = 'instagram' AND medium = 'partnerships' THEN 'Partnerships (UTM)'
      WHEN source LIKE 'email%' OR medium IN ('loops','crm') THEN 'Email'
      WHEN source IN ('chatgpt.com','perplexity.ai','gemini','claude') THEN 'SEO LLMs'
      WHEN source = 'google' AND medium = 'organic' THEN 'SEO'
      WHEN source = 'bing' AND medium = 'organic' THEN 'SEO'
      WHEN source = '(direct)' AND medium = '(none)' THEN 'Direct'
      WHEN source = 'upselling' AND medium = 'plataforma' THEN 'Plataforma'
      WHEN medium = 'new_ecommerce' THEN 'Plataforma'
      WHEN medium = 'referral' THEN 'Referral'
      WHEN source IS NULL AND medium IS NULL THEN 'Orgánico'
      ELSE 'Orgánico'
    END AS utm_subchannel,
    CASE
      WHEN source = 'adwords' AND medium = 'ppc' THEN 'Paid'
      WHEN source = 'google' AND medium = 'cpc' THEN 'Paid'
      WHEN source LIKE 'facebook%' AND medium IN ('cpc', 'paid') THEN 'Paid'
      WHEN source = 'tiktok' THEN 'Paid'
      ELSE 'Non-Paid'
    END AS utm_parent
  FROM `coderhouse-data.Finance.Purchases`
  WHERE payment_status IN ('SUCCEEDED', 'SUCCESS')
    AND created_at >= FORMAT_DATE('%Y-01-01', CURRENT_DATE())
    AND email NOT LIKE '%@coderhouse.com'
    AND email NOT LIKE '%+test%'
    AND product NOT LIKE '%(Demo)%'
),
utm_cluster AS (
  SELECT 'utm' as vista, cluster, mes, utm_subchannel as dimension, utm_parent as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 1) as revenue
  FROM base GROUP BY cluster, mes, utm_subchannel, utm_parent
),
utm_all AS (
  SELECT 'utm' as vista, 'all' as cluster, mes, utm_subchannel as dimension, utm_parent as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 1) as revenue
  FROM base GROUP BY mes, utm_subchannel, utm_parent
),
cupon_cluster AS (
  SELECT 'cupon_sub' as vista, cluster, mes, cupon_parent as dimension, '' as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 1) as revenue
  FROM base GROUP BY cluster, mes, cupon_parent
),
cupon_all AS (
  SELECT 'cupon_sub' as vista, 'all' as cluster, mes, cupon_parent as dimension, '' as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 1) as revenue
  FROM base GROUP BY mes, cupon_parent
),
det_cluster AS (
  SELECT 'cupon_det' as vista, cluster, mes, cupon_detail as dimension, cupon_parent as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 1) as revenue
  FROM base WHERE cupon_detail IS NOT NULL
  GROUP BY cluster, mes, cupon_detail, cupon_parent
),
det_all AS (
  SELECT 'cupon_det' as vista, 'all' as cluster, mes, cupon_detail as dimension, cupon_parent as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 1) as revenue
  FROM base WHERE cupon_detail IS NOT NULL
  GROUP BY mes, cupon_detail, cupon_parent
),
cruce_cluster AS (
  SELECT 'cruce' as vista, cluster, mes,
    CONCAT(cupon_parent, ' × ', utm_subchannel) as dimension,
    cupon_parent as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 0) as revenue
  FROM base GROUP BY cluster, mes, cupon_parent, utm_subchannel
  HAVING SUM(revenue) > 0
),
cruce_all AS (
  SELECT 'cruce' as vista, 'all' as cluster, mes,
    CONCAT(cupon_parent, ' × ', utm_subchannel) as dimension,
    cupon_parent as parent,
    COUNT(*) as ventas, ROUND(SUM(revenue), 0) as revenue
  FROM base GROUP BY mes, cupon_parent, utm_subchannel
  HAVING SUM(revenue) > 0
)
SELECT * FROM utm_cluster UNION ALL SELECT * FROM utm_all
UNION ALL SELECT * FROM cupon_cluster UNION ALL SELECT * FROM cupon_all
UNION ALL SELECT * FROM det_cluster UNION ALL SELECT * FROM det_all
UNION ALL SELECT * FROM cruce_cluster UNION ALL SELECT * FROM cruce_all
ORDER BY vista, cluster, mes, revenue DESC
"""

print("Querying BigQuery...")
rows = list(client.query(QUERY).result())
data = [dict(r) for r in rows]
print(f"  → {len(data)} records fetched")

# ── Patch index.html ─────────────────────────────────────────────────────────
with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

new_line = "const RAW_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
html = re.sub(r"const RAW_DATA = \[.*?\];", new_line, html, flags=re.DOTALL)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("  → index.html updated successfully")
