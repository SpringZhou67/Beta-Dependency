/* Prepare excess returns for SPY (market) and daily risk-free rate */
libname scratch '/scratch/wustl/spring_research/';
libname ff '/wrds/ff/sasdata/';

/* Sort adjusted_minute_returns by sym_root, date, and minute_time_m */
proc sort data=scratch.adjusted_minute_returns;
    by sym_root date minute_time_m;
run;

/* Extract market (SPY) returns */
data market_returns;
    set scratch.adjusted_minute_returns;
    /* Initialize variable for market returns */
    retain market_return;

    /* If the symbol is SPY, store the market return */
    if sym_root = 'SPY' then do;
        market_return = adj_iret;
        output;
    end;

    /* Keep only the relevant records */
    keep date minute_time_m market_return;
run;

/* Sort market returns data by date and minute_time_m */
proc sort data=market_returns;
    by date minute_time_m;
run;

/* Step 2: Extract daily risk-free rate from ff.factors_daily and calculate rf_rate */
data daily_rf;
    set ff.factors_daily;

    /* Compute 1-minute risk-free rate */
    rf_rate = rf / 390;

    /* Create minute_time_m for each minute from 9:30 to 15:59 */
    do i = 0 to 389;
        minute_time_m = intnx('minute', '09:30:00't, i);
        output;
    end;

    /* Keep only the necessary columns */
    keep date minute_time_m rf_rate;
run;

/* Sort daily risk-free rate data by date and minute_time_m */
proc sort data=daily_rf;
    by date minute_time_m;
run;

proc sort data=scratch.adjusted_minute_returns;
    by date minute_time_m;
run;

/* Step 4: Merge market returns with daily risk-free rates */
data scratch.excess_minute_returns;
    merge scratch.adjusted_minute_returns(in=a keep=sym_root date minute_time_m adj_iret)
          market_returns(keep=date minute_time_m market_return)
          daily_rf(keep=date minute_time_m rf_rate);
    by date minute_time_m;

    /* Compute excess returns only if all required values are present */
    if a and not missing(rf_rate) and not missing(market_return) then do;
        excess_return = adj_iret - rf_rate; /* Calculate excess return */
        excess_market_return = market_return - rf_rate; /* Calculate market excess return */
        /* Keep only the relevant variables */
        keep sym_root date minute_time_m excess_return excess_market_return;
        output; /* Output only if the condition is met */
    end;
run;

/* Step 4: Extract year from the combined returns and keep minute_time_m */
data yearly_returns;
    set scratch.excess_minute_returns;
    year = year(date); /* Extract year */
    keep sym_root year minute_time_m excess_return excess_market_return; /* Keep minute_time_m */
run;

/* Step 5: Calculate means of excess returns and market returns annually */
proc sql;
    create table annual_means_minute as
    select sym_root,
           year,
           mean(excess_return) as mean_excess_return,
           mean(excess_market_return) as mean_excess_market_return
    from yearly_returns
    group by sym_root, year;
quit;

/* Step 6: Calculate covariance and variance using a rolling window of 60 months */
proc sql;
    create table rolling_covariance_variance as
    select a.sym_root,
           year(a.date) as year,  /* Extract year from date */
           sum((a.excess_return - b.mean_excess_return) * 
               (a.excess_market_return - b.mean_excess_market_return)) / 
               (count(*) - 1) as covariance,
           sum((a.excess_market_return - b.mean_excess_market_return) ** 2) / 
               (count(*) - 1) as variance
    from scratch.excess_minute_returns as a
    inner join annual_means_minute as b
    on a.sym_root = b.sym_root and year(a.date) = b.year /* Ensure both are year */
    where a.date >= intnx('month', a.date, -60, 'b') 
    group by a.sym_root, year(a.date)
    having count(*) >= 60; /* Ensure at least 60 months of data */
quit;

/* Calculate beta */
data beta_annual_minute;
    set rolling_covariance_variance;
    if variance > 0 then beta = covariance / variance;
    else beta = .;
run;

/* Sort the final beta dataset by sym_root and year */
proc sort data=beta_annual_minute;
    by sym_root year;
run;

/* Step 1: Create the final beta dataset with shifted years */
data scratch.beta_minute;
    set beta_annual_minute;
    /* Shift the beta by one year */
    year = year + 1;  /* Shift the year by 1 */
    /* Keep only the relevant variables */
    keep sym_root year beta;
run;

/* Step 2: Sort the shifted beta dataset by sym_root and shifted_year */
proc sort data=scratch.beta_minute;
    by sym_root year;
run;

