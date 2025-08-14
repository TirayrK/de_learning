#view 1
CREATE OR REPLACE VIEW `silver_data.avg_daily_user_activity` AS
SELECT
  AVG(TotalSteps) AS average_steps,
  AVG(TotalDistance) AS average_distance,
  AVG(Calories) AS average_calories
FROM
  `my_bellabeat_dataset.daily_activity`;

SELECT * FROM `silver_data.avg_daily_user_activity`;

#view 2
CREATE OR REPLACE VIEW `silver_data.user_step_goal_completion` AS
WITH user_avg_steps AS (
  SELECT
    Id,
    AVG(TotalSteps) AS avg_steps
  FROM
    `my_bellabeat_dataset.daily_activity`
  GROUP BY
    Id
)
SELECT
  (SELECT COUNT(Id) FROM user_avg_steps) AS total_users_num,
  (SELECT COUNT(Id) FROM user_avg_steps WHERE avg_steps BETWEEN 8000 AND 12000) AS goal_completed_users_num;

SELECT * FROM `silver_data.user_step_goal_completion`;

#view 3
CREATE OR REPLACE VIEW `silver_data.weekly_step_analysis` AS
WITH daily_steps_with_day AS (
  SELECT
    TotalSteps,
    EXTRACT(DAYOFWEEK FROM datetime) AS day_num,
    FORMAT_DATE('%A', datetime) AS day_name
  FROM
    `my_bellabeat_dataset.daily_activity`
)
SELECT
  day_num AS day,
  day_name AS day_of_week,
  AVG(TotalSteps) AS avg_total_step,
  COUNTIF(TotalSteps BETWEEN 8000 AND 12000) AS no_of_count
FROM
  daily_steps_with_day
GROUP BY
  day_num,
  day_name
ORDER BY
  day_num;

SELECT * FROM `silver_data.weekly_step_analysis`;

#view 4
CREATE OR REPLACE VIEW `silver_data.hourly_step_average` AS
SELECT
  EXTRACT(HOUR FROM PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', ActivityHour)) AS hour_of_day,
  AVG(StepTotal) AS average_walk
FROM
  `my_bellabeat_dataset.hourly_steps`
GROUP BY
  hour_of_day
ORDER BY
  hour_of_day;

SELECT * FROM `silver_data.hourly_step_average`;

#view 5
CREATE OR REPLACE VIEW `silver_data.user_sleep_categories` AS
WITH user_avg_sleep AS (
  SELECT
    Id,
    AVG(TotalMinutesAsleep) AS avg_minutes_asleep
  FROM
    `my_bellabeat_dataset.sleep_day`
  GROUP BY
    Id
),
user_sleep_category AS (
  SELECT
    Id,
    CASE
      WHEN avg_minutes_asleep > 540 THEN 'Oversleeping'
      WHEN avg_minutes_asleep BETWEEN 420 AND 540 THEN 'Healthy Sleep'
      WHEN avg_minutes_asleep BETWEEN 210 AND 420 THEN 'Unhealthy Sleep'
      ELSE 'Nap'
    END AS sleep_type
  FROM
    user_avg_sleep
)
SELECT
  sleep_type,
  COUNT(Id) AS no_of_users
FROM
  user_sleep_category
GROUP BY
  sleep_type;

SELECT * FROM `silver_data.user_sleep_categories`;

#view 6
CREATE OR REPLACE VIEW `silver_data.weekly_sleep_average` AS
WITH sleep_with_day AS (
  SELECT
    TotalMinutesAsleep,
    EXTRACT(DAYOFWEEK FROM PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', SleepDay)) AS day_num,
    FORMAT_DATE('%A', DATE(PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', SleepDay))) AS day_name
  FROM
    `my_bellabeat_dataset.sleep_day`
)
SELECT
  day_num AS day,
  day_name AS day_of_week,
  AVG(TotalMinutesAsleep) AS avg_total_min_asleep,
  ROUND(AVG(TotalMinutesAsleep) / 60, 2) AS avg_total_hr_asleep
FROM
  sleep_with_day
GROUP BY
  day_num,
  day_name
ORDER BY
  day_num;

SELECT * FROM `silver_data.weekly_sleep_average`;