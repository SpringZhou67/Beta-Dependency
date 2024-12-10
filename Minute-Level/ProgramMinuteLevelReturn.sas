%macro dailytaq;
    /* Load parameters */
    %let dateprefix = %sysget(DATEPREFIX);
    %let subsample = %sysget(SUBSAMPLE); 
    %let interval_minutes = 1; /* Define the interval in minutes */

    /* Define fixed parameters */
    %let start_time = '9:30:00't;
    %let end_time = '16:00:00't; 
    
    /* Conditional setup of whereclause */
    %if %upcase(&subsample) = TEST %then %do;
        %let whereclause = 1=1;
    %end;
    %else %do;
        %let whereclause = 1; 
    %end;

    /* Print or log the parameters for verification */
    %put DATEPREFIX: &dateprefix;
    %put SUBSAMPLE: &subsample;
    %put WHERECLAUSE: &whereclause;

    /* Retrieve NBBO data */
    libname nbbo '/wrds/nyse/sasdata/taqms/nbbo';
    data NBBO;
        set nbbo.nbbom_&dateprefix:;
        where &whereclause 
            and sym_suffix = '' /* Common stocks only */
            and ("09:00:00.000000000"t <= time_m <= &end_time); 
        
        format date date9.;
        format time_m TIME20.9;
    run;

    /* Retrieve Trade data */
    libname ct '/wrds/nyse/sasdata/taqms/ct';
    data trade(keep=date minute_time_m sym_root price);
        set ct.ctm_&dateprefix:;
        where &whereclause 
            and sym_suffix = '' /* Common stocks only */
            and ("09:30:00.000000000"t <= time_m <= &end_time); 

        type = 'T'; 
        format date date9.;
        format time_m TIME20.9;

        /* Create minute-level timestamp */
        minute_time_m = intnx('MINUTE', time_m, 0, 'B');
        format minute_time_m TIME5.;
    run;

    /* Sort the data by sym_root, date, and minute_time_m */
    proc sort data=trade;
        by sym_root date minute_time_m;
    run;

    /* Keep only the last record for each minute */
    data trade_last;
        set trade;
        by sym_root date minute_time_m;

        if last.minute_time_m;  /* Keep the last record for each minute */

        keep date minute_time_m sym_root price;
    run;


    /* Clean and prepare NBBO data */
    data NBBO;
        set NBBO;
        where Qu_Cond in ('A','B','H','O','R','W') /* Filter quote conditions */
            and Qu_Cancel ne 'B' /* Exclude canceled quotes */
            and not (Best_Ask le 0 and Best_Bid le 0) /* Exclude both bid and ask being 0 or missing */
            and not (Best_Asksiz le 0 and Best_Bidsiz le 0) /* Exclude both bid and ask size being 0 or missing */
            and not (Best_Ask = . and Best_Bid = .) /* Exclude both bid and ask being missing */
            and not (Best_Asksiz = . and Best_Bidsiz = .); /* Exclude both bid and ask size being missing */

        /* Create spread and midpoint */
        Spread = Best_Ask - Best_Bid;
        Midpoint = (Best_Ask + Best_Bid) / 2;

        /* Convert bid/ask sizes from round lots to shares */
        Best_BidSizeShares = Best_BidSiz * 100;
        Best_AskSizeShares = Best_AskSiz * 100;
        
            /* Create minute-level timestamp */
        minute_time_m = intnx('MINUTE', time_m, 0, 'B');
        format minute_time_m TIME5.;

        keep date minute_time_m sym_root Midpoint;
    run;

    /* Sort NBBO by sym_root, date, and minute_time_m */
    proc sort data=NBBO;
        by sym_root date minute_time_m;
    run;

    /* Keep only the last record for each minute in NBBO data */
    data NBBO_last;
        set NBBO;
        by sym_root date minute_time_m;

        /* Retain only the last record per minute */
        if last.minute_time_m;

        keep date minute_time_m sym_root Midpoint;
    run;

    proc sort data=trade_last;
        by sym_root date minute_time_m;
    run;
    
    /* Merge NBBO and Trade data */
    data MinuteReturns_raw;
        merge NBBO_last(in=a) trade_last(in=b);
        by sym_root date minute_time_m;

        if a and b; /* Only keep records present in both datasets */

        keep sym_root date minute_time_m price Midpoint;
    run;

    /* Sort MinuteReturns_raw by sym_root, date, and time_m */
    proc sort data=MinuteReturns_raw;
        by sym_root date minute_time_m;
    run;

    /* Identify the last record per minute */
    data MinuteReturns_last;
        set MinuteReturns_raw;
        by sym_root date minute_time_m;

        /* Retain the last price of each minute */
        retain last_price;
        if last.minute_time_m then last_price = price;

        keep sym_root date minute_time_m last_price Midpoint; 
    run;
    
    /* Calculate minute-level returns */
    data MinuteReturns_minute;
        set MinuteReturns_last;
        by sym_root date minute_time_m;
        
        /* Ensure last_price is assigned if it is missing */
        if missing(last_price) then last_price = Midpoint;

        /* Initialize variables */
        retain prev_price .;

        /* Calculate return */
        if first.sym_root then do;
            iret = .; /* Missing value for the first minute */
        end;
        else do;
            /* Calculate return using the previous price */
            if not missing(prev_price) then do;
                iret = log(last_price) - log(prev_price);
            end;
            else do;
                iret = .; 
            end;
        end;

        /* Update previous price after calculating iret */
        prev_price = last_price;

        keep sym_root date minute_time_m iret last_price;
    run;

    /* Sort MinuteReturns_minute*/
    proc sort data=MinuteReturns_minute;
        by sym_root date minute_time_m;
    run;

    /* Output the minute return panel */
    libname output '/scratch/wustl/spring_research/minreturn_folder'; 
    data output.MinuteReturns_&dateprefix;
        set MinuteReturns_minute;
        keep sym_root date minute_time_m iret last_price;
    run;

%mend dailytaq;

%dailytaq;
