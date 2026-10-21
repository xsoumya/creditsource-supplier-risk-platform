-- =====================================================================
-- Chart-of-accounts label mapping — seed data
-- Used by pipelines/supplier_financials_pipeline.py (silver_line_items).
-- label_pattern is a lower-case regex matched against normalised labels.
-- Lower priority number wins when several labels map to one field.
-- sign = -1 flips expenses reported as positive numbers where needed.
-- =====================================================================

USE CATALOG creditsource;
CREATE SCHEMA IF NOT EXISTS ref;

CREATE OR REPLACE TABLE ref.coa_mapping (
  label_pattern    STRING,
  canonical_field  STRING,
  sign             INT,
  priority         INT
);

INSERT INTO ref.coa_mapping VALUES
  -- Profit and loss
  ('^(total )?revenue$',                                   'revenue',                       1, 1),
  ('^(total )?(sales|turnover|revenue from .*)$',          'revenue',                       1, 2),
  ('^gross (profit|margin)$',                              'gross_profit',                  1, 1),
  ('^(normalised|adjusted|underlying) ebitda$',            'ebitda_normalised',             1, 1),
  ('^ebitda$',                                             'ebitda_normalised',             1, 2),
  ('^depreciation( and|&| &) amortisation( expense)?$',   'depreciation_amortisation',     1, 1),
  ('^(interest|finance) (expense|costs?)$',                'interest_expense',              1, 1),
  ('^(net )?profit( / |/| )?\\(?loss\\)? before (income )?tax$', 'npbt',                  1, 1),
  ('^(profit|loss) before tax$',                           'npbt',                          1, 2),
  ('^(net )?profit( / |/| )?\\(?loss\\)? after (income )?tax$',  'npat',                  1, 1),
  ('^net (profit|income)$',                                'npat',                          1, 2),
  -- Balance sheet: assets
  ('^cash( and|&| &) cash equivalents$',                   'cash',                          1, 1),
  ('^cash( at bank)?$',                                    'cash',                          1, 2),
  ('^(trade )?(receivables|debtors)$',                     'trade_receivables',             1, 1),
  ('^inventor(y|ies)$|^stock( on hand)?$',                 'inventory',                     1, 1),
  ('^total current assets$',                               'total_current_assets',          1, 1),
  ('^total non ?-?current assets$',                        'total_non_current_assets',      1, 1),
  ('^total assets$',                                       'total_assets',                  1, 1),
  -- Balance sheet: liabilities and equity
  ('^(trade )?(payables|creditors)$',                      'trade_payables',                1, 1),
  ('^total current liabilities$',                          'total_current_liabilities',     1, 1),
  ('^total non ?-?current liabilities$',                   'total_non_current_liabilities', 1, 1),
  ('^total liabilities$',                                  'total_liabilities',             1, 1),
  ('^(borrowings|interest ?-?bearing (liabilities|debt)|bank loans?)$', 'interest_bearing_debt', 1, 1),
  ('^(loans? from|amounts? (due|owing) to) related part(y|ies)$',      'related_party_loans',   1, 1),
  ('^(issued|share|contributed) capital$',                 'share_capital',                 1, 1),
  ('^total equity$',                                       'total_equity',                  1, 1),
  ('^net assets$',                                         'total_equity',                  1, 2),
  -- Cash flow
  ('^net cash (from|provided by|used in) operating activities$', 'operating_cash_flow',     1, 1);
