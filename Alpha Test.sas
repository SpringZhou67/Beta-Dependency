libname scratch '/scratch/wustl/spring_research/';

/* Step 1: Ensure data is sorted by portfolio_label */
proc sort data=scratch.all_daily_returns;
    by portfolio_label;
run;

/* Step 2: CAPM Regression for Daily Returns (Value-Weighted) */
proc reg data=scratch.all_daily_returns outest=daily_results_value_weighted;
    model value_weighted_return = excess_market_return;
    by portfolio_label; 
    ods output ParameterEstimates=daily_value_weighted;
run;

/* Step 3: Ensure data is sorted by portfolio_label for minute returns */
proc sort data=scratch.all_minute_returns;
    by portfolio_label;
run;

/* Step 4: CAPM Regression for Minute-Level Returns (Value-Weighted) */
proc reg data=scratch.all_minute_returns outest=minute_results_value_weighted;
    model value_weighted_return = excess_market_return;
    by portfolio_label; /* Separate by portfolio */
    ods output ParameterEstimates=minute_value_weighted; /* Output alpha/beta estimates */
run;

/* Step 5: Consolidate Results for Daily and Minute-Level Value-Weighted Returns */
data alpha_summary_value_weighted;
    merge daily_value_weighted (rename=(Estimate=Daily_Alpha StdErr=Daily_SE ProbT=Daily_p_value))
          minute_value_weighted (rename=(Estimate=Minute_Alpha StdErr=Minute_SE ProbT=Minute_p_value));
    by portfolio_label;
    where Variable = "Intercept";
    length Return_Frequency $10;

    /* Calculate t-statistics for Daily and Minute Alphas */
    Daily_t_statistic = Daily_Alpha / Daily_SE;
    Minute_t_statistic = Minute_Alpha / Minute_SE;

    /* Indicate the frequency */
    Return_Frequency = "Value-Weighted";

    keep portfolio_label Daily_Alpha Daily_SE Daily_p_value Daily_t_statistic 
         Minute_Alpha Minute_SE Minute_p_value Minute_t_statistic Return_Frequency;
run;

/* Sort alpha_summary_value_weighted by portfolio_label */
proc sort data=alpha_summary_value_weighted;
    by portfolio_label;
run;

/* Step 6: Display the Results for Value-Weighted Returns in a Table */
proc print data=alpha_summary_value_weighted label noobs;
    title "CAPM Alpha Summary (Daily and Minute Alphas) - Value-Weighted Returns";
    var portfolio_label 
        Daily_Alpha Daily_SE Daily_t_statistic Daily_p_value 
        Minute_Alpha Minute_SE Minute_t_statistic Minute_p_value;
run;

/* Step 7: CAPM Regression for Daily Returns (Equal-Weighted) */
proc reg data=scratch.all_daily_returns outest=daily_results_equal_weighted;
    model equal_weighted_return = excess_market_return;
    by portfolio_label;
    ods output ParameterEstimates=daily_equal_weighted; /* Output alpha/beta estimates */
run;

/* Step 8: CAPM Regression for Minute-Level Returns (Equal-Weighted) */
proc reg data=scratch.all_minute_returns outest=minute_results_equal_weighted;
    model equal_weighted_return = excess_market_return;
    by portfolio_label; /* Separate by portfolio */
    ods output ParameterEstimates=minute_equal_weighted; /* Output alpha/beta estimates */
run;

/* Step 9: Consolidate Results for Daily and Minute-Level Equal-Weighted Returns */
data alpha_summary_equal_weighted;
    merge daily_equal_weighted (rename=(Estimate=Daily_Alpha StdErr=Daily_SE ProbT=Daily_p_value))
          minute_equal_weighted (rename=(Estimate=Minute_Alpha StdErr=Minute_SE ProbT=Minute_p_value));
    by portfolio_label;
    where Variable = "Intercept"; 
    length Return_Frequency $10;

    /* Calculate t-statistics for Daily and Minute Alphas */
    Daily_t_statistic = Daily_Alpha / Daily_SE;
    Minute_t_statistic = Minute_Alpha / Minute_SE;

    /* Indicate the frequency */
    Return_Frequency = "Equal-Weighted";

    keep portfolio_label Daily_Alpha Daily_SE Daily_p_value Daily_t_statistic 
         Minute_Alpha Minute_SE Minute_p_value Minute_t_statistic Return_Frequency;
run;

/* Sort alpha_summary_equal_weighted by portfolio_label */
proc sort data=alpha_summary_equal_weighted;
    by portfolio_label;
run;

/* Step 10: Display the Results for Equal-Weighted Returns in a Table */
proc print data=alpha_summary_equal_weighted label noobs;
    title "CAPM Alpha Summary (Daily and Minute Alphas) - Equal-Weighted Returns";
    var portfolio_label 
        Daily_Alpha Daily_SE Daily_t_statistic Daily_p_value 
        Minute_Alpha Minute_SE Minute_t_statistic Minute_p_value;
run;

/* Step 11A: Compute Differences in Alphas for Value-Weighted Returns */
data alpha_diff_value_weighted;
    set alpha_summary_value_weighted;
    Alpha_Difference = Daily_Alpha - Minute_Alpha;
    StdErr_Difference = sqrt(Daily_SE**2 + Minute_SE**2); /* Assuming independence */
    t_statistic = Alpha_Difference / StdErr_Difference;
    p_value = 2 * (1 - probt(abs(t_statistic), _N_ - 1)); /* Two-tailed test */
run;

proc print data=alpha_diff_value_weighted label noobs;
    title "Differences in CAPM Alphas (Value-Weighted Returns)";
    var portfolio_label Alpha_Difference StdErr_Difference t_statistic;
run;

/* Step 11B: Compute Differences in Alphas for Equal-Weighted Returns */
data alpha_diff_equal_weighted;
    set alpha_summary_equal_weighted;
    Alpha_Difference = Daily_Alpha - Minute_Alpha;
    StdErr_Difference = sqrt(Daily_SE**2 + Minute_SE**2); /* Assuming independence */
    t_statistic = Alpha_Difference / StdErr_Difference;
    p_value = 2 * (1 - probt(abs(t_statistic), _N_ - 1)); /* Two-tailed test */
run;

/* Display the Results for Alpha Differences in Equal-Weighted Portfolios */
proc print data=alpha_diff_equal_weighted label noobs;
    title "Differences in CAPM Alphas (Equal-Weighted Returns)";
    var portfolio_label Alpha_Difference StdErr_Difference t_statistic;
run;

/* Step 12: Add Paired T-Test Statistics to Alpha Summary for Value-Weighted Returns */
data alpha_summary_value_weighted;
    set alpha_summary_value_weighted;
    /* Compute paired t-test statistics */
    Alpha_Difference = Daily_Alpha - Minute_Alpha;
    StdErr_Difference = sqrt(Daily_SE**2 + Minute_SE**2); /* Assuming independence */
    t_statistic = Alpha_Difference / StdErr_Difference;

    /* Compute p-value for two-tailed test */
    p_value_diff = 2 * (1 - probt(abs(t_statistic), _N_ - 1)); /* Adjust degrees of freedom as needed */

    /* Retain original columns and add new ones */
    label 
        Daily_Alpha = "Daily Alpha"
        Minute_Alpha = "Minute Alpha"
        Daily_SE = "Daily Alpha StdErr"
        Minute_SE = "Minute Alpha StdErr"
        Daily_p_value = "Daily Alpha p-value"
        Minute_p_value = "Minute Alpha p-value"
        Alpha_Difference = "Difference (Daily - Minute)"
        StdErr_Difference = "StdErr of Difference"
        t_statistic = "T-Statistic"
        p_value_diff = "Paired T-Test p-value";
run;

/* Step 13A: Display Daily and Minute Alpha Results (Top Table) */
proc print data=alpha_summary_value_weighted label noobs;
    title "CAPM Alpha Summary (Daily and Minute Alphas) - Value-Weighted Returns";
    var portfolio_label 
        Daily_Alpha Daily_SE Daily_t_statistic Daily_p_value 
        Minute_Alpha Minute_SE Minute_t_statistic Minute_p_value;
run;

/* Step 13B: Display Alpha Difference and T-Test Results (Bottom Table) */
proc print data=alpha_summary_value_weighted label noobs;
    title "Paired T-Test Results (Alpha Difference) - Value-Weighted Returns";
    var portfolio_label 
        Alpha_Difference StdErr_Difference t_statistic p_value_diff;
run;

/* Step 14A: Add Paired T-Test Statistics to Alpha Summary for Equal-Weighted Returns */
data alpha_summary_equal_weighted;
    set alpha_summary_equal_weighted;
    /* Compute paired t-test statistics */
    Alpha_Difference = Daily_Alpha - Minute_Alpha;
    StdErr_Difference = sqrt(Daily_SE**2 + Minute_SE**2); /* Assuming independence */
    t_statistic = Alpha_Difference / StdErr_Difference;

    /* Compute p-value for two-tailed test */
    p_value_diff = 2 * (1 - probt(abs(t_statistic), _N_ - 1)); /* Adjust degrees of freedom as needed */

    /* Retain original columns and add new ones */
    label 
        Daily_Alpha = "Daily Alpha"
        Minute_Alpha = "Minute Alpha"
        Daily_SE = "Daily Alpha StdErr"
        Minute_SE = "Minute Alpha StdErr"
        Daily_p_value = "Daily Alpha p-value"
        Minute_p_value = "Minute Alpha p-value"
        Alpha_Difference = "Difference (Daily - Minute)"
        StdErr_Difference = "StdErr of Difference"
        t_statistic = "T-Statistic"
        p_value_diff = "Paired T-Test p-value";
run;

/* Step 14B: Display Daily and Minute Alpha Results (Top Table) */
proc print data=alpha_summary_equal_weighted label noobs;
    title "CAPM Alpha Summary (Daily and Minute Alphas) - Equal-Weighted Returns";
    var portfolio_label 
        Daily_Alpha Daily_SE Daily_t_statistic Daily_p_value 
        Minute_Alpha Minute_SE Minute_t_statistic Minute_p_value;
run;

/* Step 15: Display Alpha Difference and T-Test Results (Bottom Table) */
proc print data=alpha_summary_equal_weighted label noobs;
    title "Paired T-Test Results (Alpha Difference) - Equal-Weighted Returns";
    var portfolio_label 
        Alpha_Difference StdErr_Difference t_statistic p_value_diff;
run;




