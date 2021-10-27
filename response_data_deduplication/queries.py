"""Queries DB"""
from config import DATABASE_RAW, TABLE_RESPONSE_DATA, TABLE_DUPLICATES


def query_prev_dups(year: str, month: str, day: str) -> str:
    """Retrieve duplicates within the the last 3 months vs today"""
    return f"""
WITH prev_duplicates AS (
  SELECT uuid, year, month ,day
  FROM {DATABASE_RAW}.{TABLE_RESPONSE_DATA}
  WHERE year = '{year}' and month = '{month}' and day = '{day}'
  AND uuid IN (
    SELECT uuid
    FROM {DATABASE_RAW}.{TABLE_RESPONSE_DATA}
    WHERE date(concat(year, '-', month, '-', day))  > (date(concat({year}, '-', {month}, '-', {day})) - interval '3' month)
    AND date(concat(year, '-', month, '-', day)) < date(concat({year}, '-', {month}, '-', {day}))
    )  
), 

dup_ranked AS (
    SELECT uuid
         , (row_number() OVER (PARTITION BY uuid)) as duplicate_count
         , year, month, day, hour
    FROM {DATABASE_RAW}.{TABLE_RESPONSE_DATA}
    WHERE year = '{year}' and month = '{month}' and day = '{day}'
)

SELECT uuid, uuid as original_uuid, duplicate_count, CAST('by_id' AS varchar) duplicate_type, year, month, day, hour
FROM dup_ranked
WHERE duplicate_count > 0
"""


def query_duplicate_table() -> str:
    return f"""
SELECT uuid, max(duplicate_count)
FROM {DATABASE_RAW}.{TABLE_DUPLICATES}
WHERE date(concat(year, '-', month, '-', day))  > (current_date - interval '3' month)
GROUP BY uuid
"""
