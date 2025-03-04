-- Make sure MySQL Server allows 
SHOW VARIABLES LIKE 'local_infile';

SET GLOBAL local_infile = 1;

DROP DATABASE IF EXISTS `world_layoffs`;
CREATE DATABASE `world_layoffs`;
USE `world_layoffs`;

CREATE TABLE layoffs (
    company VARCHAR(255),
    location VARCHAR(255),
    industry VARCHAR(255),
    total_laid_off INT,
    percentage_laid_off FLOAT,
	date DATE,
    stage VARCHAR(50), 
    country VARCHAR(255),
    funds_raised BIGINT
    );
    
    
select *
from layoffs;

--  ------- HANDLING MISSING VALUES --------
SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs                       -- 1211 rows affected
SET total_laid_off = 0
WHERE total_laid_off IS NULL;

UPDATE layoffs						-- 1196 rows affected
SET percentage_laid_off = 0
WHERE percentage_laid_off IS NULL;

--  ----CONVERT DATA INTO DATE FORMAT (if not already) -----------
UPDATE layoffs
SET date = DATE(date);

-- ----------NORMALIZING INDUSTRY & LOCATION NAMES ----------------
-- to fix inconsistencies
UPDATE layoffs
SET industry = LOWER(industry);            -- 3293 rows affected


-- ===================== BASIC ANALYSIS SECTION =====================
-- 1. Find Total layoffs per Company
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs
GROUP BY company
ORDER BY total_layoffs DESC;


-- 2. Find Layoffs by Industry
SELECT industry, SUM(total_laid_off) as total_layoffs 
FROM layoffs
GROUP BY industry
ORDER BY total_layoffs DESC;

-- 3. Find layoffs by Country
SELECT country, SUM(total_laid_off) as total_layoffs
FROM layoffs
GROUP BY country
ORDER BY total_layoffs DESC
;

-- 4. Find layoffs by Month (Monthly Trend)
SELECT DATE_FORMAT(date, '%Y-%m') AS month, SUM(total_laid_off) AS total_layoffs
FROM layoffs
GROUP BY month
ORDER BY month DESC
LIMIT 100
;

-- 5. Find Companies with Highest Percentage of Layoff
SELECT company, percentage_laid_off
FROM layoffs
WHERE percentage_laid_off IS NOT NULL
ORDER BY percentage_laid_off DESC
LIMIT 10;

-- 6. Find the Impact of Funding on layoffs
SELECT company, funds_raised, total_laid_off
FROM layoffs
WHERE funds_raised IS NOT NULL
ORDER BY funds_raised DESC;

-- 7. Find layoffs by stage.
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM layoffs
GROUP BY stage
ORDER BY total_layoffs DESC;

-- ================ ADVANCED ANALYSIS =====================
-- ============== WINDOWS FUNCTIONS ======================
-- Tracking cumulative layoffs over time to observe trends.(Windows Function)
WITH Layoff_Trend AS (
SELECT date, SUM(total_laid_off) AS daily_layoffs
FROM layoffs
WHERE total_laid_off IS NOT NULL
GROUP BY date
)
SELECT date, 
		daily_layoffs, 
        SUM(daily_layoffs) OVER(ORDER BY date) AS cumulative_layoffs
FROM Layoff_Trend;


-- Identify Companies that had layoffs in consecutive MONTHS (using Windows Function and LAG)
-- CTE (Monthly_Layoffs) aggregates total layoffs by company and month, and converts date into year-month format
WITH Monthly_Layoffs AS (
	SELECT company,
		   DATE_FORMAT(date, '%Y-%m') AS month,
           SUM(total_laid_off) AS total_layoffs
	FROM layoffs
    WHERE total_laid_off IS NOT NULL
    GROUP BY company, month
)
SELECT *
FROM (
	SELECT company,
			month,
			total_layoffs,
			LAG(month) OVER (PARTITION BY company ORDER BY month) AS prev_month
	FROM Monthly_Layoffs
    ) subquery      -- subquery applies LAG(month) to campute the previous month foe each company
WHERE prev_month IS NOT NULL; -- we cannot use WHERE in the subquery section

-- =========================== Self-join =======================================================
-- Examine whether companies that raised funds experienced layoffs before or after the event.
SELECT l1.company,
	   l1.date AS layoff_date,
       l2.date AS funding_date,
       l2.total_laid_off,
       l2.funds_raised,
       DATEDIFF(l1.date, l2.date) AS days_difference
FROM layoffs l1
JOIN layoffs l2
	  ON l1.company = l2.company
      AND l2.funds_raised IS NOT NULL
WHERE l1.total_laid_off IS NOT NULL
ORDER BY company, days_difference
LIMIT 1000;


-- INDUSTRY LAYOFF TRENDS: Year-over_Year (CTE & WINDOWS FUNCTION)
-- To Show how layoffs in each industry changed compared to the previous year.
WITH Industry_Layoffs AS (
	SELECT industry,
		   DATE_FORMAT(date, '%Y') AS year,
           SUM(total_laid_off) AS yearly_layoffs
	FROM layoffs
    WHERE total_laid_off IS NOT NULL
    GROUP BY industry, year
)
SELECT industry,
		year,
        yearly_layoffs,
        LAG(yearly_layoffs) OVER(PARTITION BY industry ORDER BY year) AS prev_year_layoffs,
        (yearly_layoffs - LAG(yearly_layoffs) OVER (PARTITION BY industry ORDER BY year)) AS yoy_change
FROM Industry_Layoffs ;


-- To Find Percentage of Workforce Laid Off vs. Funding Raised
-- To Rank companies based on their layoffs relative to workforce size, by considering their fundings too
SELECT company,
		industry,
        total_laid_off,
        percentage_laid_off,
        funds_raised,
        RANK() OVER(ORDER BY percentage_laid_off DESC) AS layoff_rank,
        RANK() OVER(ORDER BY funds_raised DESC) AS funding_rank
FROM layoffs
WHERE total_laid_off IS NOT NULL AND percentage_laid_off IS NOT NULL
ORDER BY layoff_rank;


-- To Identify Companies with Layoffs Across Multiple Locations (Using Self-join)
SELECT DISTINCT l1.company, 
				l1.location AS location_1,
                l2.location AS location_2,
FROM layoffs l1
JOIN layoffs l2
	ON l1.company = l2.company
    AND l1.location <> l2.location ;
				

        




