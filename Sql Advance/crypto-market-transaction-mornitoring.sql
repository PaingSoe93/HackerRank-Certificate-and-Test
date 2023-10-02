WITH RankedTransactions AS (
    SELECT
        *,
        LAG(dt) OVER (PARTITION BY sender ORDER BY dt) AS prev_dt,
        CASE
            WHEN LAG(dt) OVER (PARTITION BY sender ORDER BY dt) IS NULL THEN 1
            WHEN TIMESTAMPDIFF(SECOND, LAG(dt) OVER (PARTITION BY sender ORDER BY dt), dt) > 3600 THEN 1
            ELSE 0
        END AS new_sequence
    FROM transactions
),
SequenceGroups AS (
    SELECT
        *,
        SUM(new_sequence) OVER (PARTITION BY sender ORDER BY dt) AS sequence_group
    FROM RankedTransactions
),
SuspiciousSequences AS (
    SELECT
        sender,
        MIN(dt) AS sequence_start,
        MAX(dt) AS sequence_end,
        COUNT(*) AS transactions_count,
        ROUND(SUM(amount), 6) AS transactions_sum
    FROM SequenceGroups
    GROUP BY sender, sequence_group
    HAVING COUNT(*) >= 2 AND SUM(amount) >= 150
)
SELECT
    sender,
    sequence_start,
    sequence_end,
    transactions_count,
    transactions_sum
FROM SuspiciousSequences
ORDER BY sender, sequence_start, sequence_end;
