use world_layoffs;
select * from layoffs;

-- 1.Remove duplicates
-- 2.standardize the data
-- 3.remove null or blank
-- 4.remove column

CREATE TABLE layoffs_stagging like layoffs;
select * from layoffs_stagging;

-- inserting values in to layoffs_stagging table same as layoffs table:
insert layoffs_stagging
select * from layoffs; 

-- remove duplicate:

with CTE_DUPLICATE 
as
(
select *,
row_number() over 
(partition by company ,location, industry , total_laid_off , percentage_laid_off , `date` , 
stage , country , funds_raised_millions) 
as row_num
from layoffs_stagging
)
select * from
cte_duplicate where row_num > 1;

-- create another table to delete duplicate 
CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_stagging2  
select *,
row_number() over 
(partition by company ,location, industry , total_laid_off , percentage_laid_off , `date` , 
stage , country , funds_raised_millions) 
as row_num
from layoffs_stagging;

select * from layoffs_stagging2;

delete 
from layoffs_stagging2 
where row_num > 1;

-- 2. STANDARDIZING THE DATA
-- column company
select company , trim(company) from  layoffs_stagging2;

update layoffs_stagging2 
set company = trim(company);
-- column location
select distinct location from layoffs_stagging2 order by 1;
-- no issue

-- column industry
select distinct industry from layoffs_stagging2 order by 1;

select industry
from layoffs_stagging2
where industry like 'crypto%';

update layoffs_stagging2
set industry = 'crypto'
where industry like 'crypto%';

-- column country
select distinct country from layoffs_stagging2 order by 1;

select distinct country , trim(trailing '.' from country ) from layoffs_stagging2 order by 1;

update layoffs_stagging2
set country = trim(trailing '.' from country)
where country like 'united states%';

-- date column (changing format from string to date)


update layoffs_stagging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select * from layoffs_stagging2;

alter table layoffs_stagging2
modify column `date` date;

-- remove null

select * 
from layoffs_stagging2
where industry is null
or industry = '';

select * from layoffs_stagging2 where company = 'airbnb';

update layoffs_stagging2
set industry = null
where industry = '';

select * from
layoffs_stagging2 tb1 
join layoffs_stagging2 tb2
	on tb1.company = tb2.company and
    tb1.location = tb2.location
where tb1.industry is null
and tb2.industry is not null;

update  
layoffs_stagging2 tb1 
join layoffs_stagging2 tb2
on tb1.company = tb2.company 
set tb1.industry = tb2.industry
where tb1.industry is null
and tb2.industry is not null; 

select * from layoffs_stagging2
where total_laid_off is null and
percentage_laid_off is null;

delete from layoffs_stagging2
where total_laid_off is null and
percentage_laid_off is null;

select * from layoffs_stagging2;
-- remove unnecessary column/row

alter table layoffs_stagging2
drop column row_num;







