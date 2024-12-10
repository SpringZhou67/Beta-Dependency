/* Step 1: Load the Data */
libname scratch '/scratch/wustl/spring_research/';

/* Step 2: Extract and Rename Betas */
data beta_daily_revised;
    set scratch.beta_daily;
    where year between 2022 and 2024; 
    keep sym_root year beta;
    rename beta=beta_daily;
run;

data beta_minute_revised;
    set scratch.beta_minute;
    where year between 2022 and 2024;
    keep sym_root year beta;
    rename beta=beta_minute;
run;

/* Sort datasets before merging */
proc sort data=beta_daily_revised;
    by sym_root year;
run;

proc sort data=beta_minute_revised;
    by sym_root year;
run;

   /* Step 3: Merge the datasets */
data beta_difference;
    merge beta_daily_revised beta_minute_revised;
    by sym_root year;
    if not missing(beta_minute) and not missing(beta_daily) then do;
        beta_diff = beta_minute - beta_daily;
    end;
run;


/* Step 4: Segment into Portfolios */
proc rank data=beta_difference out=ranked_betas groups=5 descending;
    var beta_diff;
    ranks portfolio;
run;

/* Then assign labels directly */
data final_portfolios;
    length portfolio_label $10;
    set ranked_betas;
    if not missing(portfolio) then do;
        portfolio_label = strip(put(portfolio + 1, best12.)); /* Convert to string and increment rank */
    end;
run;

/* Step 6: Extract Last Market Cap of the Previous Year per sym_root */
proc sql;
    create table last_market_cap_prev_year as
    select sym_root, 
           int(year(date)) + 1 as year, /* Assign market cap to the following year */
           market_cap
    from scratch.excess_daily_returns
    group by sym_root, year(date)
    having date = max(date); /* Select the last date of the year */
quit;

/* Step 7: Merge Portfolios with Market Cap, Exclude Empty Portfolio Labels */
proc sql;
    create table portfolio_market_cap as
    select a.sym_root, 
           a.year, 
           a.portfolio_label, 
           b.market_cap
    from final_portfolios as a
    inner join last_market_cap_prev_year as b
    on a.sym_root = b.sym_root and a.year = b.year
    where not missing(a.portfolio_label);
quit;

/* Step 8: Filter and Merge Minute Returns with Portfolio Data */
proc sql;
    create table portfolio_minute_returns as
    select a.date, a.sym_root, a.minute_time_m, a.excess_return, a.excess_market_return,
           b.portfolio_label, 
           b.market_cap
    from scratch.excess_minute_returns as a
    inner join portfolio_market_cap as b
    on a.sym_root = b.sym_root and year(a.date) = b.year
    where not missing(b.portfolio_label) 
          and a.date between '01JAN2022'd and '05DEC2024'd; 
quit;

/* Step 9: Calculate Minute-Level Portfolio Returns */
proc sql;
    create table portfolio_level_minute_returns as
    select date, 
           minute_time_m,
           portfolio_label, 
           excess_market_return,
           mean(excess_return) as equal_weighted_return,
           sum(excess_return * market_cap) / sum(market_cap) as value_weighted_return
    from (select distinct date, minute_time_m, portfolio_label, excess_return, market_cap, excess_market_return
          from portfolio_minute_returns) as sub
    group by date, minute_time_m, portfolio_label;
quit;

proc sort data=portfolio_level_minute_returns nodupkey;
    by date minute_time_m portfolio_label;
run;

/* Step 10: Calculate 5-1 Minute Returns */
proc sql;
    create table long_short_minute_returns as
    select a.date, a.excess_market_return,
           a.minute_time_m,
           (a.equal_weighted_return - b.equal_weighted_return) as equal_weighted_return,
           (a.value_weighted_return - b.value_weighted_return) as value_weighted_return
    from portfolio_level_minute_returns as a
    inner join portfolio_level_minute_returns as b
    on a.date = b.date 
       and a.minute_time_m = b.minute_time_m
       and a.portfolio_label = "5"
       and b.portfolio_label = "1";
quit;

/* Step 11: Combine Minute Returns, Exclude Empty Portfolio Labels */
data scratch.all_minute_returns;
    set portfolio_level_minute_returns
        long_short_minute_returns (in=ls);
    if ls then portfolio_label = "5-1";
    if not missing(portfolio_label); /* Exclude rows with missing portfolio labels */
run;

/* Step 12: Merge Daily Returns with Portfolio Data */
proc sql;
    create table portfolio_daily_returns as
    select a.date, a.sym_root, a.excess_return, a.excess_market_return,
           b.portfolio_label, b.market_cap
    from scratch.excess_daily_returns as a
    inner join portfolio_market_cap as b
    on a.sym_root = b.sym_root and year(a.date) = b.year
    where not missing(b.portfolio_label) 
          and a.date between '01JAN2022'd and '05DEC2024'd; 
quit;

/* Step 13: Calculate Daily-Level Portfolio Returns */
proc sql;
    create table portfolio_level_daily_returns as
    select date, portfolio_label, excess_market_return,
           mean(excess_return) as equal_weighted_return,
		   sum(excess_return * market_cap) / sum(market_cap) as value_weighted_return
    from (select distinct date, portfolio_label, excess_return, market_cap, excess_market_return
          from portfolio_daily_returns) as sub
    group by date, portfolio_label;
quit;

proc sort data=portfolio_level_daily_returns nodupkey;
    by date portfolio_label;
run;

/* Step 14: Calculate 5-1 Daily Returns */
proc sql;
    create table long_short_daily_returns as
    select a.date, a.excess_market_return,
           (a.equal_weighted_return - b.equal_weighted_return) as equal_weighted_return,
           (a.value_weighted_return - b.value_weighted_return) as value_weighted_return
    from portfolio_level_daily_returns as a
    inner join portfolio_level_daily_returns as b
    on a.date = b.date
       and a.portfolio_label = "5" 
       and b.portfolio_label = "1";
quit;

/* Step 15: Combine Daily Returns, Exclude Empty Portfolio Labels */
data scratch.all_daily_returns;
    set portfolio_level_daily_returns
        long_short_daily_returns (in=ls);
    if ls then portfolio_label = "5-1";
    if not missing(portfolio_label); /* Exclude rows with missing portfolio labels */
run;

/* Step 16: Final Sorting for all returns and long_short dataset */
proc sort data=scratch.all_minute_returns;
    by portfolio_label date minute_time_m;
run;

proc sort data=scratch.all_daily_returns;
    by portfolio_label date;
run;


