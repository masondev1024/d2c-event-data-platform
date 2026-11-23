-- Bind only the positive BIGINT snapshot ID reported by the local Spark commit.
-- Keep NULLs and every key, including unexpected windows and duplicate copies.
SELECT window_key, product_id, window_state, entry_count, count(*) AS copies
FROM raffle_mart FOR VERSION AS OF {snapshot_id}
GROUP BY window_key, product_id, window_state, entry_count
