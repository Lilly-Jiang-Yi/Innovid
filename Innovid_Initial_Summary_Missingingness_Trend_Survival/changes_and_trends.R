rm(list=ls())
gc(reset = TRUE)
options(scipen = 999)

library(rmarkdown)
library(data.table)
library(kableExtra)
library(callr)
library(poLCA)
library(R.devices)
library(DBI)
library(RPostgres)
library(qualvar)
library(DBI)
library(aws.signature)
library("bit64")
library(RAthena)
library(reticulate)
library(strucchange)

reticulate::use_condaenv()


########################################
# BEGIN: Parameter Section
########################################
# Load data from file (if false, then downlaod from Athena)
load_from_file = TRUE

# Set directory location. Not sure if going forward we will
# be using this 'prefix'. We may remove depending on how our
# code structure evolves.
prefix  <- "./"

dep_var      = "impressions" 
ylab         = "# Impressions"
main         = "Time Series of Impressions"
report_title = "Changes and Trends Report for Impressions"

dep_var      = "completion100"
ylab         = "# Completions"
main         = "Time Series of Completions"
report_title = "Changes and Trends Report for Completions"

dep_var      = "completion100"
ylab         = "# Click Throughs"
main         = "Time Series of Click Throughs"
report_title = "Changes and Trends Report for Click Throughs"

# Independent variables
date_var  = "date"
m_var     = c("publisher", "format", "duration", "placement")

# When we condition out subsets of data from an independent variable,
# we could produce a plot for all levels of that variable. But that 
# can be huge. Sometimes it is better to constrain that plot by the top
# most frequent levels. This parameter fixes the total number of possible plots
# per level. If you really want to see all, use "Inf" (without quotes).
mxplots_per_iv = 5

# output dir: make sure to add slash.
outputdir = 'OUTPUT/'

########################################
# END: Parameter Section
########################################

# Code will append ".html" to name.
outputname = paste0("changes_and_trends_", dep_var)

source(paste0(prefix, 'standard_config.R'))
source(paste0(prefix, 'FUNCTIONS/Functions.R'))


origquery = 
"select 
  date, 
  duration,
  publisher,
  format,
  placement,
  sum(impressions) as impressions,
  sum(clickthroughs) as clickthroughs,
  sum(completion25) as completion25,
  sum(completion50) as completion50,
  sum(completion75) as completion75,
  sum(completion100) as completion100
from 
  raw_consumer.innovid
where 
  lower(campaign) like '%trulicity%'
group by 
  date, 
  duration,
  publisher,
  format,
  placement
"

## We can also run queries in the background if we need to:
#tmpres = r_bg(athena_background, list(origquery, prefix))
#dat0 = tmpres$get_result()
#fwrite(dat0, paste0(prefix, 'DAT/all_data_date_efficient.csv'))

if(load_from_file == FALSE) {
  use_credentials(file = AWS_keys_location)  
  
  con <- dbConnect(RAthena::athena(),
                   s3_staging_dir = s3_staging_dir,
                   region_name    = 'us-east-2'
  )
  
  (dat0     <- dbGetQuery(con, origquery))
  fwrite(dat0, paste0(prefix, 'DAT/all_data_date_efficient.csv'))
  
} else {
  dat0 <- fread(paste0(prefix, 'DAT/all_data_date_efficient.csv'))
}
  
dat0[, date := as.Date(date, '%d/%b/%Y')]

dt_range = c(min(dat0$date, na.rm = TRUE), max(dat0$date, na.rm = TRUE))

# Convert apply bit64 integer conversion to large integers.
dat0[] <- lapply(dat0, 
  function(x) { 
    if(is.integer64(x)) {
      x = bit64::as.integer.integer64(x) 
    }
    return(x) 
  }
)

# All_res is a list which will contain all the objects necessary
# to build the report. Each element is saved and pass into a params
# object at the end of the code to 
all_res <- list()

pream = "<h1>Summary</h1>
<p>
The report in here summarizes changes and trends. In here, we identify a 
dependent variable and evaluate it varies across time. We also condition out
or condition on the data to identify any important covariates that contribute
to the overall series.
</p>
<p>
For the time series plots, we present change points (red vertical lines) that
signify possible changes in the time series. This analysis does not identify 
the cause for those change points but suggests possible areas for investigation,
or at least discussion with subject matter expert on configuration.
</p>

<p>
<br><b>Unit of Analysis:</b> Raw events rows
<br><b>Dates:</b> DTMIN - DTMAX
</p>
"

pream = gsub("DTMIN", dt_range[1], pream)
pream = gsub("DTMAX", dt_range[2], pream)

# This code creates a summary statement
all_res[[length(all_res)+1]] <- list(preamble = pream)

# We create a table of the timeseries. 
t1 <- prep_table(dep_var, date_var)

# Pass it to our plotting routine. THis routine will 
# capture the image object and save it into all_res.
# This function is saved under FUNCTIONS/Functions.R
img <- plot_time_with_bp_capture(
  t1, 
  ylab          = ylab, 
  main          = main,
  ylim          = c(0, max(t1$V1)),
  change_points = TRUE,
  dt_range      = dt_range
)
all_res[[length(all_res)+1]] <- list(img = img)
  
for(j in 1:length(m_var)) {
  
  all_res[[length(all_res)+1]] <- list(
    separator = paste0("Conditioning out data by the variable:",  m_var[j])
  )
  
  ivars <- c(date_var, m_var[j])
  tt1   <- dat0[, sum(get(dep_var)), mget(ivars)]
  nms   <- tt1[, sum(V1), get(m_var[j])]
  setorder(nms, -V1)
  
  if(nrow(nms) > mxplots_per_iv) { 
    len = mxplots_per_iv
  } else {
    len = nrow(nms)
  }
  
  nms <- nms[, get][1:len]
  
  for(i in 1:len) {
    res = plot_time_with_bp_capture(
      tt1[get(m_var[j]) == nms[i], .(date, V1)], 
      ylab     = ylab, 
      main     = paste0(main, "\n", m_var[j], ": ", nms[i]),
      ylim     = c(0, tt1[get(m_var[j]) == nms[i], max(V1)]),
      dt_range = dt_range
    )
    all_res[[length(all_res)+1]] <- list(img = res)
  }
}

outputname1 = paste0(outputname, '.html')
params <- list()
params$all_res <- all_res

params$set_title <- report_title
render(
  paste0(prefix, 'TEMPLATES/image_template_qual_var.Rmd'),
  output_file = outputname1,
  output_dir  = paste0(prefix, outputdir),
  quiet       = FALSE,
  params      = params
)


file.show(paste0(prefix, outputdir, outputname1))

