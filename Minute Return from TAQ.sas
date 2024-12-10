/* Define libnames for datasets */
libname scratch '/scratch/wustl/spring_research';
libname crsp '/wrds/crsp/sasdata/a_stock';

/* Prepare the DSENAME dataset to retain only the latest permno */
data latest_permno_dsenames;
    set crsp.dsenames;
    rename ticker=sym_root;
    format namedt date9. nameendt date9.;
run;

/* Sort by sym_root and descending namedt */
proc sort data=latest_permno_dsenames;
    by sym_root descending namedt;
run;

/* Retain the latest permno for each sym_root */
data latest_permno_dsenames;
    set latest_permno_dsenames;
    by sym_root descending namedt;
    if first.sym_root;
run;

/* Sort combined minute return data */
proc sort data=scratch.combined_minutereturns;
    by sym_root date;
run;

/* Merge with the latest permno */
data combined_with_permno;
    merge scratch.combined_minutereturns (in=a)
          latest_permno_dsenames (in=b);
    by sym_root;
    if a and b; /* Keep only matching records */
run;

/* Step 3: Sort the merged data by sym_root, date, and minute_time_m */
proc sort data=combined_with_permno;
    by sym_root date minute_time_m;
run;

/* Read SHROUT and PRC values from crsp.dsf dataset */
data cfacpr_data;
    set crsp.dsf (keep=permno date cfacpr openprc prc SHROUT);
    format date date9.;
run;

/* Merge combined_with_permno with cfacpr_data */
proc sql;
    create table merged_data as
    select a.*, b.prc, b.openprc, b.cfacpr
    from combined_with_permno as a
    left join cfacpr_data as b
    on a.permno = b.permno and a.date = b.date;
quit;

/* Ensure both datasets are sorted by the BY variables */
proc sort data=merged_data;
    by sym_root date minute_time_m;
run;

/* Merge cfacpr values with the adjusted minute returns dataset */
data adjusted_minutereturns;
    set merged_data;
    by sym_root date;
    
    retain last_price;
    
    /* For the first record of each day or symbol, set initial values */
    if first.sym_root or first.date then do;
        last_price = openprc;  /* Set initial price to open price for the first minute */
    end;
    
    /* For the last record of each day or symbol, set the final values */
    if last.sym_root or last.date then do;
        last_price = prc;  /* Set last price to close price for the last minute */
    end;

    /* Calculate adjusted price using cfacpr */
    if not missing(last_price) and not missing(cfacpr) and cfacpr ne 0 then do;
        prc_adj = last_price / cfacpr;
    end;
    else do;
        prc_adj = last_price; 
    end;

    if not missing(prc_adj) and not missing(lag(prc_adj)) then do;
        adj_iret = (prc_adj - lag(prc_adj)) / lag(prc_adj);  /* Normal adjusted return */
    end;

    /* Store the prc_adj of the last minute of the previous day for future use */
    if first.sym_root then do;
		adj_iret=.;
    end;

    /* Keep necessary variables */
    keep permno sym_root date minute_time_m prc_adj previous_prc_adj adj_iret;
run;

/* Sort the final adjusted minute returns dataset by permno, date, and minute_time_m */
proc sort data=adjusted_minutereturns;
    by permno date minute_time_m;
run;

/* Merge with dsedelist to get delisting date */
data scratch.adjusted_minute_new;
    merge adjusted_minutereturns (in=a)
          crsp.dsedelist (keep=permno dlstdt DLRET);
    by permno;

    if a; 

    /* Adjust minute-level returns for delisting events */
    if date >= dlstdt then do;
        if DLRET ne . and DLRET > 0 then do;
            adj_iret = DLRET; 
        end;
        else do;
            adj_iret = .;
        end;
    end;
    
    drop dlstdt DLRET;
run;

/* Sort the final adjusted minute return dataset */
proc sort data=scratch.adjusted_minute_new;
    by sym_root date minute_time_m;
run;



