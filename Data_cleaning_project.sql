-- Data cleaning 

select *
from layoffs;

-- step 1 remove duplicates
-- step 2 standardize data
-- step 3 look at null vlaues 
-- step 4 remove unneccessary columns or rows


create table layoffs_staging 
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select * 
from layoffs;


-- Step 1 removing duplicates

select * ,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, date) as Row_num
from layoffs_staging2
;

with duplicate_cte as
(
select * ,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as dup
#remember to use `` when partitioning date because date is a keyword
from layoffs_staging2
)
select * 
from duplicate_cte
where dup > 1;


select *
from layoffs_staging
where company = 'casper';


with duplicate_cte as
(
select * ,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as Row_numb
#remember to use `` when partitioning date because date is a keyword
from layoffs_staging2
)
#cannot use delete in a cte 
select *
from duplicate_cte
where row_numb > 1;


CREATE TABLE `layoffs_staging3` (##creating a new table to take care of duplicates 
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int, #added a row number to show what might be a duplicate
  `dup` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging3
where row_num > 1; #showing the table with the row numbers

insert into layoffs_staging3
select * ,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as dup
from layoffs_staging2;

delete  ## this deletes the duplicates 
from layoffs_staging3
where dup > 1;   

select *
from layoffs_staging2; ##double checks the new table 

-- Standardizing data
## finding issues in the data and fixing it 

select distinct company, trim(company)
from layoffs_staging2;

update layoffs_staging2 ## updates the table with the trimmmed company tab
set company = trim(company)
;

select distinct industry #distinct shows each singular industry 
from layoffs_staging2
order by 1; # put in order by number first

select *
from layoffs_staging2
where industry like 'Crypto%'; # the like shows everything under the crypto industry 

#updating the crypto currency industry to crypto industry 
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';


#checking industry 
select distinct industry 
from layoffs_staging2;

select distinct country
from layoffs_staging2#using this to check other coloumns too 
order by 1;

select distinct country, trim(trailing '.' from country) 
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country) 
where  country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y') 
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');


alter table layoffs_staging2 # changed date text to the date type
modify column `date` date;

select * 
from layoffs_staging3
where total_laid_off is null  #checking null values
and percentage_laid_off is null;

update layoffs_staging3
set industry = null
where industry = '';

select *
from layoffs_staging3 
where industry is null
or industry = '';

select *
from layoffs_staging3
where company like 'Bally%';

select *
from layoffs_staging3 t1
join layoffs_staging3 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging3 t1
join layoffs_staging3 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select * 
from layoffs_staging3
where total_laid_off is null  #checking null values
and percentage_laid_off is null;

### incase of situation where new table has duplicates you would need to redo row 65, 83 and 90
## this allows you to create a fresh table, see the duplicates and delete them to fix the table. 


select *
from layoffs_staging3;

-- remove columns and rows needed to be removed

select * 
from layoffs_staging3
where total_laid_off is null  #checking null values
and percentage_laid_off is null;

-- removed null values 
delete
from layoffs_staging3
where total_laid_off is null  #checking null values
and percentage_laid_off is null;

alter table layoffs_staging3
drop column row_num,
drop column dup;