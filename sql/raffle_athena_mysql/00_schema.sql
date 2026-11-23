-- Run in a dedicated fixture database. All DATETIME values represent UTC.
CREATE TABLE IF NOT EXISTS raffle_products (
    product_id SMALLINT UNSIGNED PRIMARY KEY,
    CHECK (product_id BETWEEN 1 AND 100)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS raffle_windows (
    window_key CHAR(1) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    starts_at DATETIME(3) NOT NULL,
    ends_at DATETIME(3) NOT NULL,
    revision SMALLINT UNSIGNED NOT NULL,
    cutoff_entry_id INT UNSIGNED NULL,
    window_state VARCHAR(6) GENERATED ALWAYS AS
        (CASE WHEN cutoff_entry_id IS NULL THEN 'OPEN' ELSE 'CLOSED' END) STORED,
    CHECK (starts_at < ends_at),
    CHECK ((revision = 0 AND cutoff_entry_id IS NULL) OR
           (revision > 0 AND cutoff_entry_id IS NOT NULL AND cutoff_entry_id > 0))
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS raffle_entries (
    entry_id INT UNSIGNED PRIMARY KEY,
    product_id SMALLINT UNSIGNED NOT NULL,
    occurred_at DATETIME(3) NOT NULL,
    FOREIGN KEY (product_id) REFERENCES raffle_products (product_id),
    INDEX ix_raffle_product_time (product_id, occurred_at)
) ENGINE = InnoDB;

START TRANSACTION;
INSERT INTO raffle_products (product_id)
WITH RECURSIVE products (id) AS (
    SELECT 1
    UNION ALL
    SELECT id + 1 FROM products WHERE id < 100
)
SELECT id FROM products ORDER BY id
ON DUPLICATE KEY UPDATE product_id = raffle_products.product_id;

INSERT INTO raffle_windows (window_key, starts_at, ends_at, revision, cutoff_entry_id)
VALUES ('A', '2026-01-01 00:00:00', '2026-01-01 00:05:00', 0, NULL),
       ('B', '2026-01-01 00:05:00', '2026-01-01 00:10:00', 0, NULL)
ON DUPLICATE KEY UPDATE revision = raffle_windows.revision;
COMMIT;

DROP PROCEDURE IF EXISTS raffle_apply_phase;
DELIMITER //
CREATE PROCEDURE raffle_apply_phase(IN requested_phase VARCHAR(12))
MODIFIES SQL DATA
SQL SECURITY INVOKER
BEGIN
    DECLARE a_revision SMALLINT UNSIGNED;
    DECLARE b_revision SMALLINT UNSIGNED;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;
    -- All phase writers take these locks in A/B order before reading or writing.
    SELECT revision INTO a_revision FROM raffle_windows WHERE window_key = 'A' FOR UPDATE;
    SELECT revision INTO b_revision FROM raffle_windows WHERE window_key = 'B' FOR UPDATE;
    IF a_revision IS NULL OR b_revision IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Missing fixture windows';
    END IF;

    CASE requested_phase
        WHEN 'initial' THEN
            IF a_revision = 0 THEN
                INSERT INTO raffle_entries (entry_id, product_id, occurred_at)
                WITH ids AS (
                    SELECT (hi.product_id - 1) * 100 + lo.product_id AS entry_id
                    FROM raffle_products AS hi CROSS JOIN raffle_products AS lo
                ), positions AS (
                    SELECT entry_id,
                           CASE WHEN entry_id <= 4980 THEN entry_id - 1 ELSE entry_id - 4981 END AS offset_in_window,
                           CASE WHEN entry_id <= 4980 THEN 4979 ELSE 4999 END AS last_offset,
                           CASE WHEN entry_id <= 4980 THEN '2026-01-01 00:00:00'
                                ELSE '2026-01-01 00:05:00' END AS event_start
                    FROM ids
                    WHERE entry_id <= 9980
                )
                SELECT entry_id, 1 + MOD(offset_in_window, 98),
                       TIMESTAMPADD(MICROSECOND, 1000 * ((offset_in_window * 299999) DIV last_offset),
                                    CAST(event_start AS DATETIME(3)))
                FROM positions;
                UPDATE raffle_windows SET revision = 1, cutoff_entry_id = 4980 WHERE window_key = 'A';
            END IF;
        WHEN 'sealed' THEN
            IF a_revision = 0 THEN
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Apply initial before sealed';
            END IF;
            IF b_revision = 0 THEN
                UPDATE raffle_windows SET revision = 1, cutoff_entry_id = 9980 WHERE window_key = 'B';
            END IF;
        WHEN 'backfill' THEN
            IF b_revision = 0 THEN
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Apply sealed before backfill';
            END IF;
            IF a_revision = 1 THEN
                -- Reuse catalog ids 1..20 as sequence numbers for the late batch.
                INSERT INTO raffle_entries (entry_id, product_id, occurred_at)
                SELECT 9980 + product_id, CASE WHEN product_id <= 10 THEN 99 ELSE 1 END,
                       TIMESTAMPADD(SECOND, product_id, CAST('2026-01-01 00:02:00' AS DATETIME(3)))
                FROM raffle_products WHERE product_id <= 20;
                UPDATE raffle_windows SET revision = 2, cutoff_entry_id = 10000 WHERE window_key = 'A';
            END IF;
        ELSE
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Unknown fixture phase';
    END CASE;
    COMMIT;
END//
DELIMITER ;
