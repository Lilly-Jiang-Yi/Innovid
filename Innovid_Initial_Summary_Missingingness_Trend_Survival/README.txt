Innovid Exploratories
Jason Martinez, 2020-08-06
--------------------------

The code in here produces the following reports:

	'changes_and_trends_impressions.html'
	'changes_and_trends_clickthroughs.html'
	'changes_and_trends_competion100.html'
	'initial_summary_of_data.html'
	'IQV_report.html'

The directory structure is as follows:

	FUNCTIONS -- in-house function calls.
	TEMPLATES -- Rmarkdown templates 
	OUTPUT    -- the location for where the reports are saved.

R Files
	config.R (not supplied, but should define password and 
		username for connecting to redshift).

	summary_data.R                 - Code that calls summarytools::dfSummmary

	changes_and_trends.R           - Code that generates time series plots and 
                                         conditions out data by levels of
                                         independent variables.

	plot_maps.R                    - Code that generates maps using usmaps.
                                         this code also contains some hardcoded
                                         renaming of city/states computations.

	Qualitative_variation_Report.R - Code to produce IQV report.

	source_data.R                  - Staging file 

	standard_config.R              - a configuration file useful for connecting 
                                         to Athnea.