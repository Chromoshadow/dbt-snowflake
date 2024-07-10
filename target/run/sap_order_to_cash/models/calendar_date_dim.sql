
  
    

        create or replace transient table SSI_SAP_TO_SNOW.REPORT.calendar_date_dim
         as
        (WITH calendar_date_dim AS (
    SELECT
        date_seq.date_value AS Date,
        CAST(TO_CHAR(date_seq.date_value, 'YYYYMMDD') AS BIGINT) AS DateInt,
        TO_CHAR(date_seq.date_value, 'YYYYMMDD') AS DateStr,
        TO_CHAR(date_seq.date_value, 'YYYY-MM-DD') AS DateStr2,
        EXTRACT(YEAR FROM date_seq.date_value) AS CalYear,
        CASE WHEN EXTRACT(QUARTER FROM date_seq.date_value) IN (1, 2) THEN 1 ELSE 2 END AS CalSemester,
        EXTRACT(QUARTER FROM date_seq.date_value) AS CalQuarter,
        EXTRACT(MONTH FROM date_seq.date_value) AS CalMonth,
        EXTRACT(WEEK FROM date_seq.date_value) AS CalWeek,
        TO_CHAR(EXTRACT(YEAR FROM date_seq.date_value)) AS CalYearStr,
        CASE WHEN EXTRACT(QUARTER FROM date_seq.date_value) IN (1, 2) THEN '01' ELSE '02' END AS CalSemesterStr,
        CASE WHEN EXTRACT(QUARTER FROM date_seq.date_value) IN (1, 2) THEN 'S1' ELSE 'S2' END AS CalSemesterStr2,
        '0' || EXTRACT(QUARTER FROM date_seq.date_value) AS CalQuarterStr,
        'Q' || EXTRACT(QUARTER FROM date_seq.date_value) AS CalQuarterStr2,
        TO_CHAR(date_seq.date_value, 'MMMM') AS CalMonthLongStr,
        TO_CHAR(date_seq.date_value, 'MMM') AS CalMonthShortStr,
        LPAD(TO_CHAR(EXTRACT(WEEK FROM date_seq.date_value)), 2, '0') AS CalWeekStr,
        TO_CHAR(date_seq.date_value, 'EEEE') AS DayNameLong,
        TO_CHAR(date_seq.date_value, 'EEE') AS DayNameShort,
        EXTRACT(DAYOFWEEK FROM date_seq.date_value) AS DayOfWeek,
        EXTRACT(DAY FROM date_seq.date_value) AS DayOfMonth,
        DATE_PART(DAY, DATEADD(DAY, -1, DATE_TRUNC('QUARTER', date_seq.date_value))) AS DayOfQuarter,
        CASE
            WHEN EXTRACT(QUARTER FROM date_seq.date_value) IN (1, 2) THEN EXTRACT(DOY FROM date_seq.date_value)
            WHEN EXTRACT(QUARTER FROM date_seq.date_value) = 3 THEN EXTRACT(DOY FROM date_seq.date_value) - EXTRACT(DOY FROM DATEADD(DAY, -1, DATE_TRUNC('QUARTER', date_seq.date_value)))
            ELSE EXTRACT(DOY FROM date_seq.date_value) - EXTRACT(DOY FROM DATE_TRUNC('QUARTER', DATEADD(MONTH, -3, date_seq.date_value)))
        END AS DayOfSemester,
        EXTRACT(DOY FROM date_seq.date_value) AS DayOfYear,
        CASE
            WHEN EXTRACT(QUARTER FROM date_seq.date_value) IN (1, 2) THEN EXTRACT(YEAR FROM date_seq.date_value) || 'S1'
            ELSE EXTRACT(YEAR FROM date_seq.date_value) || 'S2'
        END AS YearSemester,
        EXTRACT(YEAR FROM date_seq.date_value) || 'Q' || EXTRACT(QUARTER FROM date_seq.date_value) AS YearQuarter,
        CAST(TO_CHAR(date_seq.date_value, 'YYYYMM') AS STRING) AS YearMonth,
        EXTRACT(YEAR FROM date_seq.date_value) || ' ' || TO_CHAR(date_seq.date_value, 'MMM') AS YearMonth2,
        CONCAT(TO_CHAR(EXTRACT(YEAR FROM date_seq.date_value)), LPAD(TO_CHAR(EXTRACT(WEEK FROM date_seq.date_value)), 2, '0')) AS YearWeek,
        (DATE_TRUNC('YEAR', date_seq.date_value) = date_seq.date_value) AS IsFirstDayOfYear,
        (DATEADD(DAY, -1, DATEADD(YEAR, 1, DATE_TRUNC('YEAR', date_seq.date_value))) = date_seq.date_value) AS IsLastDayOfYear,
        (EXTRACT(MONTH FROM date_seq.date_value) IN (1, 7) AND EXTRACT(DAY FROM date_seq.date_value) = 1) AS IsFirstDayOfSemester,
        ((EXTRACT(MONTH FROM date_seq.date_value) = 6 AND EXTRACT(DAY FROM date_seq.date_value) = 30) OR (EXTRACT(MONTH FROM date_seq.date_value) = 12 AND EXTRACT(DAY FROM date_seq.date_value) = 31)) AS IsLastDayOfSemester,
        (DATE_TRUNC('QUARTER', date_seq.date_value) = date_seq.date_value) AS IsFirstDayOfQuarter,
        (DATEADD(DAY, -1, DATEADD(MONTH, 3, DATE_TRUNC('QUARTER', date_seq.date_value))) = date_seq.date_value) AS IsLastDayOfQuarter,
        (DATE_TRUNC('MONTH', date_seq.date_value) = date_seq.date_value) AS IsFirstDayOfMonth,
        (LAST_DAY(date_seq.date_value) = date_seq.date_value) AS IsLastDayOfMonth,
        (DATE_TRUNC('WEEK', date_seq.date_value) = date_seq.date_value) AS IsFirstDayOfWeek,
        (DATEADD(DAY, 6, DATE_TRUNC('WEEK', date_seq.date_value)) = date_seq.date_value) AS IsLastDayOfWeek,
        ((MOD(EXTRACT(YEAR FROM date_seq.date_value), 4) = 0 AND MOD(EXTRACT(YEAR FROM date_seq.date_value), 100) != 0) OR MOD(EXTRACT(YEAR FROM date_seq.date_value), 400) = 0) AS IsLeapYear,
        (TO_CHAR(date_seq.date_value, 'EEEE') NOT IN ('Saturday', 'Sunday')) AS IsWeekDay,
        (TO_CHAR(date_seq.date_value, 'EEEE') IN ('Saturday', 'Sunday')) AS IsWeekEnd,
        DATE_TRUNC('WEEK', date_seq.date_value) AS WeekStartDate,
        DATEADD(DAY, 6, DATE_TRUNC('WEEK', date_seq.date_value)) AS WeekEndDate,
        DATE_TRUNC('MONTH', date_seq.date_value) AS MonthStartDate,
        LAST_DAY(date_seq.date_value) AS MonthEndDate,
        (EXTRACT(WEEK FROM DATEADD(DAY, -1, DATEADD(YEAR, 1, DATE_TRUNC('YEAR', date_seq.date_value)))) = 53) AS Has53Weeks
    FROM TABLE(
        GENERATOR(
            ROWCOUNT => DATE_PART('day', DATEADD(YEAR, 20, CURRENT_DATE())) 
                        - DATE_PART('day', DATEADD(YEAR, -20, CURRENT_DATE()))
        )
    ) AS gen
    JOIN LATERAL (
        SELECT DATEADD(DAY, SEQ4(), DATE_TRUNC('YEAR', DATEADD(YEAR, -20, CURRENT_DATE()))) AS date_value
        FROM TABLE(GENERATOR(ROWCOUNT => 365 * 40))
    ) AS date_seq
)

SELECT * FROM calendar_date_dim





-- Copyright 2022 Google LLC
-- Copyright 2023 DataSentics
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.




-- with calendar_date_dim AS (
--     SELECT
--         dt.col AS Date,
--         CAST(date_format(dt.col, 'yyyyMMdd') AS BIGINT) AS DateInt,
--         date_format(dt.col, 'yyyyMMdd') AS DateStr,
--         date_format(dt.col, 'yyyy-MM-dd') AS DateStr2,
--         EXTRACT(YEAR FROM dt.col) AS CalYear,
--         IF(EXTRACT(QUARTER FROM dt.col) IN (1, 2), 1, 2) AS CalSemester,
--         EXTRACT(QUARTER FROM dt.col) AS CalQuarter,
--         EXTRACT(MONTH FROM dt.col) AS CalMonth,
--         EXTRACT(WEEK FROM dt.col) AS CalWeek,
--         CAST(EXTRACT(YEAR FROM dt.col) AS STRING) AS CalYearStr,
--         IF(EXTRACT(QUARTER FROM dt.col) IN (1, 2), '01', '02') AS CalSemesterStr,
--         IF(EXTRACT(QUARTER FROM dt.col) IN (1, 2), 'S1', 'S2') AS CalSemesterStr2,
--         '0' || EXTRACT(QUARTER FROM dt.col) AS CalQuarterStr,
--         'Q' || EXTRACT(QUARTER FROM dt.col) AS CalQuarterStr2,
--         date_format(dt.col, 'MMMM') AS CalMonthLongStr,
--         date_format(dt.col, 'MMM') AS CalMonthShortStr,
--         '0' || (EXTRACT(WEEK FROM dt.col)) AS CalWeekStr,
--         date_format(dt.col, 'EEEE') AS DayNameLong,
--         date_format(dt.col, 'EEE') AS DayNameShort,
--         EXTRACT(DAYOFWEEK FROM dt.col) AS DayOfWeek,
--         dayofmonth(dt.col) AS DayOfMonth,
--         DATE_DIFF(DAY, dt.col, DATE_TRUNC('QUARTER', dt.col)) + 1 AS DayOfQuarter,
--         IF(
--             EXTRACT(QUARTER FROM dt.col) IN (1, 2),
--             dayofyear(dt.col),
--             IF(
--             EXTRACT(QUARTER FROM dt.col) = 3,
--             dayofyear(dt.col) - dayofyear(DATE_SUB(DATE_TRUNC('QUARTER', dt.col), 1)),
--             dayofyear(dt.col) - dayofyear(DATE_TRUNC('QUARTER', dt.col - INTERVAL '3' MONTH))
--             )
--         ) AS DayOfSemester,
--         dayofyear(dt.col) AS DayOfYear,
--         IF(
--             EXTRACT(QUARTER FROM dt.col) IN (1, 2),
--             EXTRACT(YEAR FROM dt.col) || 'S1',
--             EXTRACT(YEAR FROM dt.col) || 'S2'
--         ) AS YearSemester,
--         EXTRACT(YEAR FROM dt.col) || 'Q' || EXTRACT(QUARTER FROM dt.col) AS YearQuarter,
--         CAST(date_format(dt.col, 'yyyyMM') AS STRING) AS YearMonth,
--         EXTRACT(YEAR FROM dt.col) || ' ' || date_format(dt.col, 'MMM') AS YearMonth2,
--         concat(date_format(dt.col, 'yyyy'), lpad(weekofyear(dt.col), 2, '0')) AS YearWeek,
--         (DATE_TRUNC('YEAR', dt.col) = dt.col) AS IsFirstDayOfYear,
--         ((DATE_TRUNC('YEAR', dt.col) + INTERVAL '1' YEAR ) - INTERVAL 1 DAY = dt.col) AS IsLastDayOfYear,
--         (EXTRACT(MONTH FROM dt.col) IN (1, 7) AND EXTRACT(DAY FROM dt.col) = 1) AS IsFirstDayOfSemester,
--         ((EXTRACT(MONTH FROM dt.col) IN (6) AND EXTRACT(DAY FROM dt.col) IN (30))
--             OR (EXTRACT(MONTH FROM dt.col) IN (12) AND EXTRACT(DAY FROM dt.col) IN (31))) AS IsLastDayOfSemester,
--         (DATE_TRUNC('QUARTER', dt.col) = dt.col) AS IsFirstDayOfQuarter,
--         ((DATE_TRUNC('QUARTER', dt.col) + INTERVAL '3' MONTH) - INTERVAL 1 DAY = dt.col) AS IsLastDayOfQuarter,
--         (DATE_TRUNC('MONTH', dt.col) = dt.col) AS IsFirstDayOfMonth,
--         (LAST_DAY(dt.col) = dt.col) AS IsLastDayOfMonth,
--         (DATE_TRUNC('WEEK', dt.col) = dt.col) AS IsFirstDayOfWeek,
--         (DATE_TRUNC('WEEK', dt.col) + INTERVAL 6 DAY = dt.col) AS IsLastDayOfWeek,
--         ((MOD(EXTRACT(YEAR FROM dt.col), 4) = 0 AND MOD(EXTRACT(YEAR FROM dt.col), 100) != 0)
--             OR MOD(EXTRACT(YEAR FROM dt.col), 400) = 0) AS IsLeapYear,
--         (date_format(dt.col, 'EEEE') NOT IN ('Saturday', 'Sunday')) AS IsWeekDay,
--         (date_format(dt.col, 'EEEE') IN ('Saturday', 'Sunday')) AS IsWeekEnd,
--         (DATE_TRUNC('WEEK', dt.col)) AS WeekStartDate,
--         (DATE_TRUNC('WEEK', dt.col) + INTERVAL 6 DAY) AS WeekEndDate,
--         (DATE_TRUNC('MONTH',  dt.col)) AS MonthStartDate,
--         (LAST_DAY(dt.col)) AS MonthEndDate,
--         (weekofyear((DATE_TRUNC('YEAR', dt.col) + INTERVAL '1' YEAR ) - INTERVAL 1 DAY) = 53) AS Has53Weeks
--     FROM explode(
--         sequence(
--         DATE_TRUNC('YEAR', CURRENT_DATE()) - INTERVAL '20' YEAR,
--         LAST_DAY(CURRENT_DATE()) + INTERVAL '20' YEAR,
--         INTERVAL 1 DAY)
--     ) AS dt
-- )

-- SELECT * FROM calendar_date_dim
        );
      
  