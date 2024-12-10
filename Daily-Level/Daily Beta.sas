libname scratch '/scratch/wustl/spring_research/';
libname ff '/wrds/ff/sasdata/';

/* Extract daily risk-free rate */
data daily_rf;
    set ff.factors_daily;
    keep date rf;
run;

proc sort data=daily_rf;
    by date;
run;

/* Extract SPY returns */
data spy_returns;
    set scratch.daily_net_returns;
    if sym_root = 'SPY';
    keep date net_return;
run;

/* Calculate daily market return (mean SPY return) */
proc means data=spy_returns noprint;
    var net_return;
    by date;
    output out=spy_agg mean=market_return;
run;

/* Calculate mean risk-free rate */
proc means data=daily_rf noprint;
    var rf;
    by date;
    output out=rf_agg mean=rf_rate;
run;

/* Merge market and risk-free returns */
data market_free_returns_daily;
    merge spy_agg(in=a) rf_agg(in=b);
    by date;
    if a and b;
run;

proc sort data=scratch.daily_net_returns;
    by date sym_root;
run;

/* Prepare daily excess returns data */
data scratch.excess_daily_returns;
    merge scratch.daily_net_returns(in=a)
          market_free_returns_daily(keep=date market_return rf_rate);
    by date;
    if a and not missing(rf_rate) and not missing(market_return); 
    excess_return = net_return - rf_rate; 
    excess_market_return = market_return - rf_rate; 
    year = year(date);
    keep date sym_root year excess_return excess_market_return market_cap;
run;

/* Calculate annual means of excess returns */
proc sql;
    create table annual_means as
    select sym_root,
           year,
           mean(excess_return) as mean_excess_return,
           mean(excess_market_return) as mean_excess_market_return
    from scratch.excess_daily_returns
    group by sym_root, year;
quit;

/* Calculate covariance and variance using a rolling 36-month window */
proc sql;
    create table rolling_covariance_variance as
    select a.sym_root,
           year(a.date) as year,
           sum((a.excess_return - b.mean_excess_return) * 
               (a.excess_market_return - b.mean_excess_market_return)) / 
               (count(*) - 1) as covariance,
           sum((a.excess_market_return - b.mean_excess_market_return) ** 2) / 
               (count(*) - 1) as variance
    from scratch.excess_daily_returns as a
    inner join annual_means as b
    on a.sym_root = b.sym_root and a.year = b.year
    where a.date >= intnx('month', a.date, -60, 'b') 
    group by a.sym_root, year(a.date)
    having count(*) >= 60; /* Ensure at least 60 months of data */
quit;

/* Calculate beta */
data beta_annual;
    set rolling_covariance_variance;
    if variance > 0 then beta = covariance / variance;
    else beta = .;
run;

/* Sort the final beta dataset by sym_root and year */
proc sort data=beta_annual;
    by sym_root year;
run;

/* Step 1: Create the final beta dataset with shifted years */
data scratch.beta_daily;
    set beta_annual;
    /* Shift the beta by one year */
    year = year + 1;  /* Shift the year by 1 */
    /* Keep only the relevant variables */
    keep sym_root year beta;
run;

/* Step 2: Sort the shifted beta dataset by sym_root and shifted_year */
proc sort data=scratch.beta_daily;
    by sym_root year;
run;
