/* Step 1: Set up the library references */
libname crsp '/wrds/crsp/sasdata/a_stock';
libname scratch '/scratch/wustl/spring_research/';

/* Define the date range */
%let start_date = '01JAN2017'd;
%let end_date = '05DEC2024'd;

/* Step 1: Filter dsenames to get the latest PERMNO for each sym_root */
data latest_permno_dsenames;
    set crsp.dsenames;
    rename ticker=sym_root;
    format namedt date9. nameendt date9.;
run;

/* Sort by sym_root and descending namedt */
proc sort data=latest_permno_dsenames;
    by sym_root descending namedt;
run;

/* Retain the latest PERMNO for each sym_root */
data latest_permno_dsenames;
    set latest_permno_dsenames;
    by sym_root descending namedt;
    if first.sym_root;
run;

/* Step 2: Query and filter CRSP data with the latest PERMNO */
proc sql;
  create table crsp_data as
  select a.PERMNO, a.DATE, a.ret as net_return, a.prc, a.openprc, a.SHROUT, 
         b.sym_root, b.exchcd, b.shrcd
  from crsp.dsf as a
  left join latest_permno_dsenames as b
  on a.PERMNO = b.PERMNO
  where a.DATE between &start_date and &end_date
    and a.ret is not missing
    and a.prc is not missing
    and a.openprc is not missing
    and a.SHROUT is not missing
  order by b.sym_root, a.DATE;
quit;

/* Step 3: Calculate intraday and overnight returns, and market cap */
data crsp_returns;
  set crsp_data;
  INRet = abs(prc) / abs(openprc) - 1; /* Intraday return */
  ONRet = (1 + net_return) / (1 + INRet) - 1; /* Overnight return */
  market_cap = SHROUT * abs(prc) * 1000; /* Market capitalization */
run;

/* Step 4: Merge with valid symbols */
proc sql;
  create table crsp_filtered_relevant as
  select a.*
  from crsp_returns as a
  inner join 
       (select distinct sym_root from scratch.adjusted_minute_returns) as b
  on a.sym_root = b.sym_root;
quit;

/* Final Sort */
proc sort data=crsp_filtered_relevant;
  by sym_root date;
run;

/* Save the final dataset with all required fields */
data scratch.daily_net_returns;
	set crsp_filtered_relevant;
	keep sym_root date permno net_return INRet ONRet market_cap;
run;


