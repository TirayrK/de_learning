#Print Prime Numbers
WITH RECURSIVE numbers AS (
    SELECT 2 as n
    UNION ALL
    SELECT n + 1 
    FROM numbers 
    WHERE n < 1000
),
primes AS (
    SELECT n1.n
    FROM numbers n1
    WHERE NOT EXISTS (
        SELECT 1 
        FROM numbers n2 
        WHERE n2.n > 1 
          AND n2.n <= SQRT(n1.n)
          AND n1.n % n2.n = 0
    )
)
SELECT GROUP_CONCAT(n ORDER BY n SEPARATOR '&') as result
FROM primes;

#Symmetric Pairs
WITH functions_id as (
    SELECT @row_number := @row_number + 1 AS id, X, Y
    FROM Functions
    CROSS JOIN (SELECT @row_number := 0) r
    ORDER BY X)

SELECT DISTINCT f1.X, f1.Y
FROM functions_id f1
INNER JOIN functions_id f2
ON f1.X = f2.Y AND f1.Y = f2.X AND f1.id != f2.id
WHERE f1.X <= f1.Y
ORDER BY f1.X;

#Placements
SELECT s.Name
FROM Students s
JOIN Friends f ON s.ID = f.ID
JOIN Packages p1 ON s.ID = p1.ID                  
JOIN Packages p2 ON f.Friend_ID = p2.ID           
WHERE p2.Salary > p1.Salary                         
ORDER BY p2.Salary;                               

#SQL Project Planning
WITH project_starts AS (
    SELECT 
        Start_Date,
        ROW_NUMBER() OVER (ORDER BY Start_Date) as rn
    FROM Projects
    WHERE Start_Date NOT IN (SELECT End_Date FROM Projects)
),
project_ends AS (
    SELECT 
        End_Date,
        ROW_NUMBER() OVER (ORDER BY End_Date) as rn
    FROM Projects
    WHERE End_Date NOT IN (SELECT Start_Date FROM Projects)
)
SELECT 
    s.Start_Date,
    e.End_Date
FROM project_starts s
INNER JOIN project_ends e ON s.rn = e.rn
ORDER BY
    DATEDIFF(e.End_Date, s.Start_Date),
    s.Start_Date;

#Contest Leaderboard
SELECT h.hacker_id, 
       h.name,
       SUM(max_challenge_score) as total_score
FROM Hackers h
JOIN (
    SELECT hacker_id, 
           challenge_id,
           MAX(score) as max_challenge_score
    FROM Submissions 
    GROUP BY hacker_id, challenge_id
) s ON h.hacker_id = s.hacker_id
GROUP BY h.hacker_id, h.name
HAVING total_score > 0
ORDER BY total_score DESC, h.hacker_id ASC;

#Challenges
WITH challenge_counts AS (
    SELECT h.hacker_id, h.name, COUNT(*) as cnt
    FROM Hackers h
    LEFT JOIN Challenges c ON h.hacker_id = c.hacker_id
    GROUP BY h.hacker_id, h.name
)
SELECT hacker_id, name, cnt
FROM challenge_counts
WHERE cnt = (SELECT MAX(cnt) FROM challenge_counts)
   OR cnt IN (
       SELECT cnt FROM challenge_counts 
       GROUP BY cnt HAVING COUNT(*) = 1
   )
ORDER BY cnt DESC, hacker_id;

#Ollivander's Inventory
SELECT w.id, wp.age, w.coins_needed, w.power
FROM Wands w
JOIN Wands_Property wp ON w.code = wp.code
WHERE wp.is_evil = 0
  AND w.coins_needed = (
    SELECT MIN(w2.coins_needed)
    FROM Wands w2
    JOIN Wands_Property wp2 ON w2.code = wp2.code
    WHERE wp2.is_evil = 0
      AND w2.power = w.power
      AND wp2.age = wp.age
  )
ORDER BY w.power DESC, wp.age DESC;

#Top Competitors
SELECT h.hacker_id, h.name
FROM Hackers h
INNER JOIN Submissions s ON s.hacker_id = h.hacker_id
INNER JOIN Challenges c ON s.challenge_id = c.challenge_id
INNER JOIN Difficulty d ON c.difficulty_level = d.difficulty_level
WHERE s.score = d.score
GROUP BY h.hacker_id, h.name
HAVING COUNT(s.challenge_id) > 1
ORDER BY COUNT(s.challenge_id) DESC, h.hacker_id ASC;

#The Report
SELECT 
  CASE 
    WHEN g.Grade >= 8 THEN s.Name 
    ELSE 'NULL' 
  END as Name,
  g.Grade,
  s.Marks
FROM Students s
JOIN Grades g ON s.Marks BETWEEN g.Min_Mark AND g.Max_Mark
ORDER BY 
  g.Grade DESC,
  CASE 
    WHEN g.Grade >= 8 THEN s.Name
    ELSE s.Marks
  END ASC;

#Weather Observation Station 20
SELECT ROUND(
    (SELECT LAT_N 
     FROM (SELECT LAT_N, ROW_NUMBER() OVER (ORDER BY LAT_N) as rn,
                  COUNT(*) OVER() as total_count
           FROM STATION) ranked
     WHERE rn = CEIL(total_count/2.0)
    ), 4) as median_lat;

#Weather Observation Station 19
SELECT 
    ROUND(
        SQRT(
            POWER(MAX(LAT_N) - MIN(LAT_N), 2) + 
            POWER(MAX(LONG_W) - MIN(LONG_W), 2)
        ), 4
    ) AS euclidean_distance
FROM STATION;

#Weather Observation Station 18
SELECT 
    ROUND(
        ABS(MAX(LAT_N) - MIN(LAT_N)) + 
        ABS(MAX(LONG_W) - MIN(LONG_W)), 4) 
    AS manhattan_distance
FROM STATION;

#New Companies
SELECT 
    c.company_code, 
    c.founder,
    COUNT(DISTINCT lm.lead_manager_code) as total_lead_managers,
    COUNT(DISTINCT sm.senior_manager_code) as total_senior_managers,
    COUNT(DISTINCT m.manager_code) as total_managers,
    COUNT(DISTINCT e.employee_code) as total_employees
FROM Company c
LEFT JOIN Lead_Manager lm ON c.company_code = lm.company_code
LEFT JOIN Senior_Manager sm ON c.company_code = sm.company_code
LEFT JOIN Manager m ON c.company_code = m.company_code
LEFT JOIN Employee e ON c.company_code = e.company_code
GROUP BY c.company_code, c.founder
ORDER BY c.company_code;

#Binary Tree Nodes
SELECT t1.N,
    CASE 
        WHEN t1.P IS NULL THEN 'Root'
        WHEN t2.P IS NULL THEN 'Leaf'
        ELSE 'Inner'
    END AS status
FROM BST t1
LEFT JOIN BST t2
ON t1.N = t2.P
GROUP BY t1.N, t1.P                 
ORDER BY t1.N;

#Occupations
SELECT 
    MAX(CASE WHEN Occupation = 'Doctor' THEN Name END) as Doctor,
    MAX(CASE WHEN Occupation = 'Professor' THEN Name END) as Professor,
    MAX(CASE WHEN Occupation = 'Singer' THEN Name END) as Singer,
    MAX(CASE WHEN Occupation = 'Actor' THEN Name END) as Actor
FROM (
    SELECT Name, Occupation,
           ROW_NUMBER() OVER (PARTITION BY Occupation ORDER BY Name) as position
    FROM OCCUPATIONS
) ranked
GROUP BY position
ORDER BY position;

#The PADS
SELECT CONCAT(Name, '(', LEFT(Occupation, 1), ')')
FROM OCCUPATIONS
ORDER BY Name;

SELECT CONCAT('There are a total of ', COUNT(Occupation), ' ', LOWER(Occupation), 's.')
FROM OCCUPATIONS
GROUP BY Occupation
ORDER BY COUNT(Occupation), Occupation;

#Interviews
SELECT 
    c.contest_id,
    c.hacker_id, 
    c.name,
    SUM(IFNULL(submission_totals.total_submissions, 0)) as total_submissions,
    SUM(IFNULL(submission_totals.total_accepted_submissions, 0)) as total_accepted_submissions,
    SUM(IFNULL(view_totals.total_views, 0)) as total_views,
    SUM(IFNULL(view_totals.total_unique_views, 0)) as total_unique_views
FROM Contests c
JOIN Colleges col ON c.contest_id = col.contest_id
JOIN Challenges ch ON col.college_id = ch.college_id
LEFT JOIN (
    SELECT challenge_id, 
           SUM(total_submissions) as total_submissions,
           SUM(total_accepted_submissions) as total_accepted_submissions
    FROM Submission_Stats 
    GROUP BY challenge_id
) submission_totals ON ch.challenge_id = submission_totals.challenge_id
LEFT JOIN (
    SELECT challenge_id,
           SUM(total_views) as total_views,
           SUM(total_unique_views) as total_unique_views
    FROM View_Stats
    GROUP BY challenge_id
) view_totals ON ch.challenge_id = view_totals.challenge_id
GROUP BY c.contest_id, c.hacker_id, c.name
HAVING SUM(IFNULL(submission_totals.total_submissions, 0)) + 
       SUM(IFNULL(submission_totals.total_accepted_submissions, 0)) + 
       SUM(IFNULL(view_totals.total_views, 0)) + 
       SUM(IFNULL(view_totals.total_unique_views, 0)) > 0
ORDER BY c.contest_id;
