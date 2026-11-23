-- Inclusive admission cutoff and half-open UTC event-time bounds are independent.
SELECT w.window_key, p.product_id, w.window_state, w.revision, w.cutoff_entry_id,
       CASE WHEN w.window_state = 'CLOSED' THEN COUNT(e.entry_id) END AS confirmed_count
FROM raffle_products AS p
CROSS JOIN raffle_windows AS w
LEFT JOIN raffle_entries AS e
    ON e.product_id = p.product_id
   AND e.occurred_at >= w.starts_at
   AND e.occurred_at < w.ends_at
   AND e.entry_id <= w.cutoff_entry_id
GROUP BY w.window_key, p.product_id, w.window_state, w.revision, w.cutoff_entry_id
ORDER BY w.window_key, p.product_id;
