-- ============================================================================
-- PROJECT: COVID-19 Death & Vaccination Analysis
-- DATABASE: portfolio_project
-- PURPOSE: Analyze COVID-19 cases, deaths, and vaccination trends globally
-- ============================================================================

USE portfolio_project;

-- ============================================================================
-- SECTION 1: BASIC DATA EXPLORATION
-- ============================================================================

-- Query 1: Overview of COVID-19 data by location and date
-- PURPOSE: Get a quick snapshot of cases, deaths, and population
-- NORMALIZATION: Ordered by location (1) and date (2) for chronological analysis
SELECT 
    Location, 
    date, 
    total_cases, 
    new_cases, 
    total_deaths, 
    population
FROM covid_deaths
WHERE continent IS NOT NULL 
    AND continent != ''
ORDER BY 1, 2;


-- ============================================================================
-- SECTION 2: DEATH RATE ANALYSIS
-- ============================================================================

-- Query 2: Death Rate Analysis for India
-- PURPOSE: Calculate the likelihood of dying if you contract COVID-19 in India
-- FORMULA: (total_deaths / total_cases) * 100
-- NORMALIZATION: Filter by country name pattern and exclude null continents
-- INSIGHT: Shows mortality rate trend over time
SELECT 
    Location, 
    date, 
    total_cases, 
    total_deaths, 
    ROUND((total_deaths / total_cases) * 100, 2) AS death_percentage
FROM covid_deaths
WHERE location LIKE '%ndia%'
    AND continent IS NOT NULL
    AND continent != ''
    AND total_cases > 0  -- Prevent division by zero
ORDER BY 1, 2;


-- ============================================================================
-- SECTION 3: INFECTION RATE ANALYSIS
-- ============================================================================

-- Query 3: Infection Rate in India
-- PURPOSE: Show what percentage of India's population contracted COVID-19
-- FORMULA: (total_cases / population) * 100
-- NORMALIZATION: Ordered chronologically for trend analysis
-- INSIGHT: Tracks spread of virus relative to population size
SELECT 
    Location, 
    date, 
    population, 
    total_cases, 
    ROUND((total_cases / population) * 100, 4) AS infected_population_perc
FROM covid_deaths
WHERE Location LIKE '%ndia%'
    AND continent IS NOT NULL
    AND continent != ''
ORDER BY 1, 2;


-- ============================================================================
-- SECTION 4: HIGHEST INFECTION RATES BY COUNTRY
-- ============================================================================

-- Query 4: Countries with Highest Infection Rates
-- PURPOSE: Identify which countries had the highest percentage of population infected
-- NORMALIZATION: Grouped by location and population to get max values
-- INSIGHT: Shows which countries were hit hardest relative to their population
SELECT 
    Location, 
    population, 
    MAX(total_cases) AS highest_infection_count, 
    ROUND(MAX((total_cases / population) * 100), 4) AS highest_infected_population_perc
FROM covid_deaths
WHERE continent IS NOT NULL
    AND continent != ''
GROUP BY location, population
ORDER BY highest_infected_population_perc DESC;


-- ============================================================================
-- SECTION 5: DEATH COUNT ANALYSIS BY COUNTRY
-- ============================================================================

-- Query 5: Countries with Highest Total Deaths
-- PURPOSE: Rank countries by absolute number of COVID-19 deaths
-- NORMALIZATION: Filtered to exclude continental aggregates
-- INSIGHT: Shows which countries had the highest death toll
SELECT 
    Location, 
    MAX(CAST(total_deaths AS UNSIGNED)) AS highest_death_count
FROM covid_deaths
WHERE continent IS NOT NULL 
    AND continent != ''
GROUP BY Location
ORDER BY highest_death_count DESC;


-- ============================================================================
-- SECTION 6: CONTINENTAL ANALYSIS
-- ============================================================================

-- Query 6: Total Deaths by Continent
-- PURPOSE: Break down death counts by continent for macro-level analysis
-- NORMALIZATION: Grouped by continent, excluding null values
-- INSIGHT: Shows which continents were most affected
SELECT 
    continent, 
    MAX(CAST(total_deaths AS UNSIGNED)) AS total_death_count
FROM covid_deaths
WHERE continent IS NOT NULL 
    AND continent != ''
GROUP BY continent
ORDER BY total_death_count DESC;


-- Query 7: Continents with Highest Death Count (Duplicate - can be removed)
-- NOTE: This is identical to Query 6 - consider removing for efficiency
SELECT 
    continent, 
    MAX(CAST(total_deaths AS UNSIGNED)) AS highest_death_count
FROM covid_deaths
WHERE continent IS NOT NULL 
    AND continent != ''
GROUP BY continent 
ORDER BY highest_death_count DESC;


-- ============================================================================
-- SECTION 7: GLOBAL STATISTICS
-- ============================================================================

-- Query 8: Overall Global COVID-19 Statistics
-- PURPOSE: Calculate total global cases, deaths, and average death percentage
-- NORMALIZATION: Uses subquery to aggregate daily data, then calculates totals
-- INSIGHT: Provides single summary statistics for the entire pandemic
SELECT 
    SUM(total_cases) AS global_total_cases, 
    SUM(total_deaths) AS global_total_deaths, 
    ROUND(AVG(death_percentage), 2) AS avg_death_percentage
FROM 
(
    SELECT 
        date, 
        SUM(new_cases) AS total_cases, 
        SUM(new_deaths) AS total_deaths, 
        CASE 
            WHEN SUM(new_cases) > 0 THEN (SUM(new_deaths) / SUM(new_cases)) * 100
            ELSE 0
        END AS death_percentage
    FROM covid_deaths
    WHERE continent IS NOT NULL
        AND continent != ''
    GROUP BY date
) AS daily_aggregates;


-- ============================================================================
-- SECTION 8: VACCINATION ANALYSIS
-- ============================================================================

-- Query 9: Vaccination Data Preview
-- PURPOSE: Quick look at vaccination data structure
SELECT * 
FROM covid_vaccinations
LIMIT 10;


-- Query 10: Rolling Vaccination Count by Country (Using CTE)
-- PURPOSE: Calculate cumulative vaccinations over time for each country
-- NORMALIZATION: Uses CTE for better readability and performance
-- INSIGHT: Shows vaccination progress with rolling sum
-- WINDOW FUNCTION: PARTITION BY location ensures separate counts per country
WITH pop_vs_vac AS (
    SELECT 
        cd.continent, 
        cd.location, 
        cd.date, 
        cd.population, 
        cv.new_vaccinations,
        SUM(cv.new_vaccinations) OVER (
            PARTITION BY cd.location 
            ORDER BY cd.date
        ) AS rolling_people_vaccinated
    FROM covid_deaths AS cd
    JOIN covid_vaccinations AS cv
        ON cd.location = cv.location 
        AND cd.date = cv.date
    WHERE cd.continent IS NOT NULL 
        AND cd.continent != ''
)
SELECT 
    *, 
    ROUND((rolling_people_vaccinated / population) * 100, 2) AS percent_vaccinated
FROM pop_vs_vac
ORDER BY location, date;


-- ============================================================================
-- SECTION 9: TEMPORARY TABLE FOR VACCINATION PERCENTAGE
-- ============================================================================

-- Query 11: Create and Populate Temporary Table
-- PURPOSE: Store vaccination data in a reusable temp table for complex queries
-- NORMALIZATION: Drop if exists prevents errors on re-run
-- USE CASE: Useful when you need to reference this data multiple times

DROP TABLE IF EXISTS percentage_population_vaccinated;

CREATE TABLE percentage_population_vaccinated (
    continent VARCHAR(255),
    location VARCHAR(255),
    date DATE,
    population BIGINT,
    new_vaccinations BIGINT,
    rolling_people_vaccinated BIGINT
);

INSERT INTO percentage_population_vaccinated
SELECT 
    cd.continent, 
    cd.location, 
    cd.date, 
    cd.population, 
    cv.new_vaccinations,
    SUM(cv.new_vaccinations) OVER (
        PARTITION BY cd.location 
        ORDER BY cd.date
    ) AS rolling_people_vaccinated
FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
    ON cd.location = cv.location 
    AND cd.date = cv.date
WHERE cd.continent IS NOT NULL 
    AND cd.continent != '';

-- Query to use the temp table
SELECT 
    *, 
    ROUND((rolling_people_vaccinated / population) * 100, 2) AS percent_vaccinated
FROM percentage_population_vaccinated
ORDER BY location, date;


-- ============================================================================
-- SECTION 10: CREATE VIEW FOR VISUALIZATION
-- ============================================================================

-- Query 12: Create View for Tableau/Power BI
-- PURPOSE: Store query as a reusable view for data visualization tools
-- NORMALIZATION: Views are stored queries that act like tables
-- BENEFIT: No need to rewrite complex joins every time
-- USE CASE: Connect Tableau, Power BI, or other BI tools to this view

DROP VIEW IF EXISTS percent_population_vaccinated;

CREATE VIEW percent_population_vaccinated AS 
SELECT 
    cd.continent, 
    cd.location, 
    cd.date, 
    cd.population, 
    cv.new_vaccinations,
    SUM(cv.new_vaccinations) OVER (
        PARTITION BY cd.location 
        ORDER BY cd.date
    ) AS rolling_people_vaccinated
FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
    ON cd.location = cv.location 
    AND cd.date = cv.date
WHERE cd.continent IS NOT NULL 
    AND cd.continent != '';

-- Query the view
SELECT 
    *,
    ROUND((rolling_people_vaccinated / population) * 100, 2) AS percent_vaccinated
FROM percent_population_vaccinated
ORDER BY location, date;


-- ============================================================================
-- END OF ANALYSIS
-- ============================================================================