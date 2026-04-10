/*
File: warehouse/dbt/models/marts/fct_china_volatility.sql
Description: dbt model for calculating daily Supply Chain Volatility Score
Aggregates raw GDELT events to produce a daily volatility metric.

Logic:
- Groups by event date (sqldate)
- Aggregates total media mentions (indicates event severity/reach)
- Calculates average Goldstein scale (indicates event impact: -10 to +10)
- Computes volatility score: combines negative sentiment (low Goldstein)
  with high media coverage to produce a risk indicator

Volatility Score Formula:
- Normalize total mentions (0-100 scale based on percentile)
- Invert Goldstein scale (more negative = higher volatility)
- Weight: 60% sentiment impact, 40% media coverage
*/

WITH daily_aggregates AS (
    -- Aggregate raw events by date
    SELECT
        sqldate AS event_date,
        COUNT(*) AS event_count,
        SUM(nummentions) AS total_mentions,
        AVG(goldsteinscale) AS avg_goldstein_scale,
        MIN(goldsteinscale) AS min_goldstein_scale,
        MAX(goldsteinscale) AS max_goldstein_scale,
        STDDEV(goldsteinscale) AS stddev_goldstein_scale
    FROM {{ ref('raw_gdelt_events') }}
    GROUP BY sqldate
),

volatility_calc AS (
    -- Calculate volatility metrics and scoring
    SELECT
        event_date,
        event_count,
        total_mentions,
        ROUND(avg_goldstein_scale, 4) AS avg_goldstein_scale,
        ROUND(min_goldstein_scale, 4) AS min_goldstein_scale,
        ROUND(max_goldstein_scale, 4) AS max_goldstein_scale,
        ROUND(COALESCE(stddev_goldstein_scale, 0), 4) AS stddev_goldstein_scale,

        -- Sentiment impact: -1 (most negative) to +1 (most positive)
        -- Goldstein ranges from -10 to +10
        ROUND(-(avg_goldstein_scale / 10.0), 4) AS sentiment_impact,

        -- Coverage intensity: log scale to handle outliers
        -- Adding 1 to avoid log(0), multiply for sensitivity
        ROUND(LN(total_mentions + 1) / LN(100), 4) AS coverage_intensity,

        -- Volatility Score: weighted combination
        -- Higher score = higher volatility risk
        -- Range approximately 0-100
        ROUND(
            (
                -- 60% weight on negative sentiment (inverted Goldstein)
                (GREATEST(-(avg_goldstein_scale / 10.0), 0) * 60) +
                -- 40% weight on media coverage (normalized)
                (LEAST(LN(total_mentions + 1) / LN(100), 1.0) * 40)
            )::NUMERIC,
            2
        ) AS volatility_score,

        -- Risk classification based on score
        CASE
            WHEN (
                (GREATEST(-(avg_goldstein_scale / 10.0), 0) * 60) +
                (LEAST(LN(total_mentions + 1) / LN(100), 1.0) * 40)
            ) >= 50 THEN 'HIGH'
            WHEN (
                (GREATEST(-(avg_goldstein_scale / 10.0), 0) * 60) +
                (LEAST(LN(total_mentions + 1) / LN(100), 1.0) * 40)
            ) >= 25 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,

        CURRENT_TIMESTAMP AS calculated_at

    FROM daily_aggregates
)

SELECT
    event_date,
    event_count,
    total_mentions,
    avg_goldstein_scale,
    min_goldstein_scale,
    max_goldstein_scale,
    stddev_goldstein_scale,
    sentiment_impact,
    coverage_intensity,
    volatility_score,
    risk_level,
    calculated_at
FROM volatility_calc
ORDER BY event_date DESC
