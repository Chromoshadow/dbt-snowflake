CREATE OR REPLACE TABLE SSI_SAP_TO_SNOW_CLONE.REPORT.calendar_date_dim
(
  Date DATE,
  DateInt INT,
  DateStr STRING,
  DateStr2 STRING,
  CalYear INT,
  CalSemester INT,
  CalQuarter INT,
  CalMonth INT,
  CalWeek INT,
  CalYearStr STRING,
  CalSemesterStr STRING,
  CalSemesterStr2 STRING,
  CalQuarterStr STRING,
  CalQuarterStr2 STRING,
  CalMonthLongStr STRING,
  CalMonthShortStr STRING,
  CalWeekStr STRING,
  DayNameLong STRING,
  DayNameShort STRING,
  DayOfWeek INT,
  DayOfMonth INT,
  DayOfQuarter INT,
  DayOfSemester INT,
  DayOfYear INT,
  YearSemester STRING,
  YearQuarter STRING,
  YearMonth STRING,
  YearMonth2 STRING,
  YearWeek STRING,
  IsFirstDayOfYear BOOLEAN,
  IsLastDayOfYear BOOLEAN,
  IsFirstDayOfSemester BOOLEAN,
  IsLastDayOfSemester BOOLEAN,
  IsFirstDayOfQuarter BOOLEAN,
  IsLastDayOfQuarter BOOLEAN,
  IsFirstDayOfMonth BOOLEAN,
  IsLastDayOfMonth BOOLEAN,
  IsFirstDayOfWeek BOOLEAN,
  IsLastDayOfWeek BOOLEAN,
  IsLeapYear BOOLEAN,
  IsWeekDay BOOLEAN,
  IsWeekEnd BOOLEAN,
  WeekStartDate DATE,
  WeekEndDate DATE,
  MonthStartDate DATE,
  MonthEndDate DATE,
  Has53Weeks BOOLEAN
);