-- Export every admitted source row, including events in a currently OPEN window.
SELECT entry_id, product_id,
       CONCAT(DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.'),
              LEFT(DATE_FORMAT(occurred_at, '%f'), 3), 'Z') AS occurred_at
FROM raffle_entries
ORDER BY entry_id;
