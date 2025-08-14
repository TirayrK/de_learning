#daily_activity
ALTER TABLE `my_bellabeat_dataset.daily_activity`
ADD COLUMN datetime DATE;
UPDATE `my_bellabeat_dataset.daily_activity`
SET datetime = PARSE_DATE('%m/%d/%Y', ActivityDay)
WHERE true;

#daily_calories
ALTER TABLE `my_bellabeat_dataset.daily_calories`
ADD COLUMN datetime DATE;
UPDATE `my_bellabeat_dataset.daily_calories`
SET datetime = PARSE_DATE('%m/%d/%Y', ActivityDay)
WHERE true;

#daily_intenstes
ALTER TABLE `my_bellabeat_dataset.daily_intensities`
ADD COLUMN datetime DATE;
UPDATE `my_bellabeat_dataset.daily_intensities`
SET datetime = PARSE_DATE('%m/%d/%Y', ActivityDay)
WHERE true;

#daily_steps
ALTER TABLE `my_bellabeat_dataset.daily_steps`
ADD COLUMN datetime DATE;
UPDATE `my_bellabeat_dataset.daily_steps`
SET datetime = PARSE_DATE('%m/%d/%Y', ActivityHour)
WHERE true;

#heartrate_seconds
ALTER TABLE `my_bellabeat_dataset.heartrate_seconds`
ADD COLUMN datetime DATETIME;
UPDATE `my_bellabeat_dataset.heartrate_seconds`
SET datetime = PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', Time)
WHERE true;

#hourly_calories
ALTER TABLE `my_bellabeat_dataset.hourly_calories`
ADD COLUMN datetime DATETIME;
UPDATE `my_bellabeat_dataset.hourly_calories`
SET datetime = PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', ActivityHour)
WHERE true;

#hourly_intensities
ALTER TABLE `my_bellabeat_dataset.hourly_intensities`
ADD COLUMN datetime DATETIME;
UPDATE `my_bellabeat_dataset.hourly_intensities`
SET datetime = PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', ActivityHour)
WHERE true;

#hourly_steps
ALTER TABLE `my_bellabeat_dataset.hourly_steps`
ADD COLUMN datetime DATETIME;
UPDATE `my_bellabeat_dataset.hourly_steps`
SET datetime = PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', ActivityHour)
WHERE true;

#sleep_day
ALTER TABLE `my_bellabeat_dataset.sleep_day`
ADD COLUMN datetime DATETIME;
UPDATE `my_bellabeat_dataset.sleep_day`
SET datetime = PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', SleepDay)
WHERE true;

#weight_log
ALTER TABLE `my_bellabeat_dataset.weight_log`
ADD COLUMN datetime DATETIME;
UPDATE `my_bellabeat_dataset.weight_log`
SET datetime = PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', Date)
WHERE true;





