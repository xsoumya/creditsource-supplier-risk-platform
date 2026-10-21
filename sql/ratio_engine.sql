-- =====================================================================
-- CreditSource-style ratio engine (sample) — Databricks SQL / Spark SQL
-- Reads normalised statements from SILVER, writes ratios to GOLD.
-- Catalog/schema names are illustrative; adjust to your Unity Catalog.
-- =====================================================================

USE CATALOG creditsource;

-- ---------------------------------------------------------------------
-- 0. Expected silver inputs (created by the Lakeflow/DLT pipeline)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.financial_statements (
  abn                          STRING,
  company_name                 STRING,
  anzsic_division              STRING,   -- industry, used later for benchmarking
  fy_end                       DATE,
  source                       STRING,   -- 'audited', 'reviewed', 'management'
  -- profit and loss
  revenue                      DECIMAL(18,2),
  gross_profit                 DECIMAL(18,2),
  ebitda_normalised            DECIMAL(18,2),
  depreciation_amortisation    DECIMAL(18,2),
  interest_expense             DECIMAL(18,2),
  npbt                         DECIMAL(18,2),
  npat                         DECIMAL(18,2),
  -- balance sheet
  cash                         DECIMAL(18,2),
  trade_receivables            DECIMAL(18,2),
  inventory                    DECIMAL(18,2),
  total_current_assets         DECIMAL(18,2),
  total_non_current_assets     DECIMAL(18,2),
  total_assets                 DECIMAL(18,2),
  trade_payables               DECIMAL(18,2),
  total_current_liabilities    DECIMAL(18,2),
  total_non_current_liabilities DECIMAL(18,2),
  total_liabilities            DECIMAL(18,2),
  interest_bearing_debt        DECIMAL(18,2),  -- bank debt + leases
  related_party_loans          DECIMAL(18,2),
  share_capital                DECIMAL(18,2),
  total_equity                 DECIMAL(18,2),
  -- cash flow
  operating_cash_flow          DECIMAL(18,2)
);

CREATE TABLE IF NOT EXISTS silver.assessment_orders (
  order_id         STRING,
  abn              STRING,
  contract_value   DECIMAL(18,2),   -- total contract value
  contract_years   DECIMAL(5,2),    -- term, used to annualise
  ordered_at       TIMESTAMP
);

-- ---------------------------------------------------------------------
-- 1. Core ratios per supplier per financial year  ->  gold.supplier_ratios
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE gold.supplier_ratios AS
WITH base AS (
  SELECT
    *,
    -- data-quality check: assets should equal liabilities + equity (±1%)
    abs(total_assets - (total_liabilities + total_equity))
      <= 0.01 * greatest(abs(total_assets), 1)            AS balance_check_ok,
    row_number() OVER (PARTITION BY abn ORDER BY fy_end DESC) AS year_rank
  FROM silver.financial_statements
),
ratios AS (
  SELECT
    abn,
    company_name,
    anzsic_division,
    fy_end,
    year_rank,                       -- 1 = latest year
    source,
    balance_check_ok,

    -- Liquidity
    try_divide(total_current_assets, total_current_liabilities)              AS current_ratio,
    try_divide(total_current_assets - coalesce(inventory, 0),
               total_current_liabilities)                                    AS quick_ratio,
    try_divide(cash, total_current_liabilities)                              AS cash_ratio,
    total_current_assets - total_current_liabilities                         AS working_capital,

    -- Leverage / solvency
    try_divide(interest_bearing_debt + coalesce(related_party_loans, 0),
               total_equity)                                                 AS debt_to_equity,
    try_divide(total_liabilities, total_assets)                              AS gearing,          -- liabilities / assets
    try_divide(total_equity, total_assets)                                   AS equity_ratio,
    try_divide(ebitda_normalised, interest_expense)                          AS interest_cover,
    try_divide(interest_bearing_debt - cash, ebitda_normalised)              AS net_debt_to_ebitda,

    -- Profitability
    try_divide(gross_profit, revenue)                                        AS gross_margin,
    try_divide(ebitda_normalised, revenue)                                   AS ebitda_margin,
    try_divide(npbt, revenue)                                                AS npbt_margin,
    try_divide(npat, total_assets)                                           AS return_on_assets,
    try_divide(npat, total_equity)                                           AS return_on_equity,

    -- Efficiency (days)
    try_divide(trade_receivables, revenue) * 365                             AS debtor_days,
    try_divide(trade_payables, revenue - gross_profit) * 365                 AS creditor_days,

    -- Cash quality
    try_divide(operating_cash_flow, npat)                                    AS cash_conversion,

    -- Raw values carried forward for trend + capacity checks
    revenue,
    ebitda_normalised,
    npbt,
    total_equity,
    operating_cash_flow
  FROM base
)
SELECT
  r.*,
  -- Year-on-year trends
  try_divide(revenue - lag(revenue) OVER w, abs(lag(revenue) OVER w))        AS revenue_growth,
  try_divide(total_equity - lag(total_equity) OVER w,
             abs(lag(total_equity) OVER w))                                  AS equity_growth,
  CASE WHEN npbt < 0 AND lag(npbt) OVER w < 0 THEN true ELSE false END       AS consecutive_losses,
  current_timestamp()                                                        AS computed_at
FROM ratios r
WINDOW w AS (PARTITION BY abn ORDER BY fy_end);

-- ---------------------------------------------------------------------
-- 2. Rule-based flags (thresholds kept in one place for analysts to tune)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW ratio_thresholds AS
SELECT * FROM VALUES
  ('current_ratio',   1.0,  'below'),
  ('quick_ratio',     0.8,  'below'),
  ('debt_to_equity',  2.0,  'above'),
  ('gearing',         0.8,  'above'),
  ('interest_cover',  2.0,  'below'),
  ('npbt_margin',     0.0,  'below'),
  ('cash_conversion', 0.5,  'below')
AS t(metric, threshold, direction);

CREATE OR REPLACE TABLE gold.supplier_ratio_flags AS
WITH latest AS (
  SELECT * FROM gold.supplier_ratios WHERE year_rank = 1
),
long AS (   -- unpivot latest ratios into metric/value rows
  SELECT abn, company_name, fy_end, metric, value
  FROM latest
  UNPIVOT (value FOR metric IN (
    current_ratio, quick_ratio, debt_to_equity, gearing,
    interest_cover, npbt_margin, cash_conversion
  ))
)
SELECT
  l.abn,
  l.company_name,
  l.fy_end,
  l.metric,
  round(l.value, 3) AS value,
  t.threshold,
  CASE
    WHEN t.direction = 'below' AND l.value < t.threshold THEN 'RED'
    WHEN t.direction = 'above' AND l.value > t.threshold THEN 'RED'
    WHEN t.direction = 'below' AND l.value < t.threshold * 1.25 THEN 'AMBER'
    WHEN t.direction = 'above' AND l.value > t.threshold * 0.75 THEN 'AMBER'
    ELSE 'GREEN'
  END AS flag
FROM long l
JOIN ratio_thresholds t USING (metric);

-- ---------------------------------------------------------------------
-- 3. Contract capacity — sizes risk against the specific contract value
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE gold.contract_capacity AS
SELECT
  o.order_id,
  o.abn,
  r.company_name,
  o.contract_value,
  o.contract_years,
  o.contract_value / greatest(o.contract_years, 1)                      AS annual_contract_value,
  try_divide(o.contract_value / greatest(o.contract_years, 1), r.revenue) AS contract_to_revenue,
  try_divide(o.contract_value, r.total_equity)                          AS contract_to_equity,
  r.working_capital,
  CASE
    WHEN r.total_equity <= 0                                                     THEN 'HIGH'
    WHEN try_divide(o.contract_value / greatest(o.contract_years, 1), r.revenue) > 0.5 THEN 'HIGH'
    WHEN try_divide(o.contract_value / greatest(o.contract_years, 1), r.revenue) > 0.25 THEN 'MODERATE'
    ELSE 'LOW'
  END                                                                   AS capacity_risk,
  r.fy_end                                                              AS financials_as_at
FROM silver.assessment_orders o
JOIN gold.supplier_ratios r
  ON o.abn = r.abn AND r.year_rank = 1;

-- ---------------------------------------------------------------------
-- 4. Optional: smoke-test data (run once in a dev schema)
-- ---------------------------------------------------------------------
-- INSERT INTO silver.financial_statements VALUES
--  ('11111111111','Acme Civil Pty Ltd','E','2023-06-30','audited',
--   42000000, 9800000, 3900000, 600000, 350000, 2950000, 2100000,
--   2100000, 5600000, 900000, 9800000, 4100000, 13900000, 4300000,
--   7200000, 2600000, 9800000, 3100000, 400000, 100, 4100000, 2600000),
--  ('11111111111','Acme Civil Pty Ltd','E','2024-06-30','audited',
--   47500000, 10300000, 4200000, 650000, 410000, 3140000, 2250000,
--   2600000, 6400000, 1000000, 11200000, 4400000, 15600000, 5100000,
--   8300000, 2800000, 11100000, 3400000, 350000, 100, 4500000, 3000000);
-- INSERT INTO silver.assessment_orders VALUES
--  ('ORD-001','11111111111', 12000000, 2, current_timestamp());
