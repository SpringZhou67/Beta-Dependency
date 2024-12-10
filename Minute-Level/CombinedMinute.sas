/* Step 1: Define libraries */
libname input '/scratch/wustl/spring_research/minreturn_folder';
libname output '/scratch/wustl/spring_research';

/* Step 2: Create a combined dataset from all input datasets */
data output.combined_minutereturns;
   set input.minutereturns_:;
run;

/* Step 3: Delete the original datasets in the input library */
proc datasets library=input nolist;
   delete minutereturns_:; 
quit;

