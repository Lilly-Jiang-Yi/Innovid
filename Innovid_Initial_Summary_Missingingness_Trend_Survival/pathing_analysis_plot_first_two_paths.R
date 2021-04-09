rm(list=ls())
gc(reset = TRUE)
options(scipen = 999)

download_data = FALSE

# The purpose of this code is to analyze the click through rate for the first 
# two impressions. In here, we look at the unique first two impressions, as a 
# part of our pathing analysis.

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
library(summarytools)

reticulate::use_condaenv()

prefix  <- "./"

source(paste0(prefix, 'standard_config.R'))
source("FUNCTIONS/Functions.R")
source("FUNCTIONS/Functions_unique_path.R")

# This function will read the credentials from downloads folder. I believe it 
# loads the creds as a system variable, which is then passed transparently to
# "dbConnect" below.
use_credentials(file=AWS_keys_location)

###########################################################
# Get some high level stats
###########################################################
if(download_data == TRUE) {
  con <- dbConnect(RAthena::athena(),
                   s3_staging_dir=s3_staging_dir,
                   region_name='us-east-2'
  )
  
  (dbListObjects(con))
  
  
  origquery <- "select format1, publisher1, campaign1,
                      count(*) as N,
                       sum(click1) as clicked
                    from raw_consumer.yj_invd_first5_imp
                    where
                      placement1 is not null
                    group by format1, publisher1, campaign1"
  fx = system.time({
    (dat00      <- dbGetQuery(con, origquery))
  })
  
  origquery <- "select format1, publisher1, campaign1,
                       format2, publisher2, campaign2,
                       count(*) as N,
                       sum(click2) as clicked
                    from raw_consumer.yj_invd_first5_imp
                    where
                      placement2 is not null
                    group by format1, publisher1, campaign1,
                       format2, publisher2, campaign2"
  fx = system.time({
    (dat01      <- dbGetQuery(con, origquery))
  })
  
  fwrite(dat00, 'DAT/dotplots_1_imp.csv')
  fwrite(dat01, 'DAT/dotplots_2_imp.csv')
} else {
  dat00 <- fread('DAT/dotplots_1_imp.csv')
  dat01 <- fread('DAT/dotplots_2_imp.csv')
}

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# LOGIC AND APPROACH FOR HYPOHTESIS TESTING:
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Parameters for null hypothesis testing.... These numbers refer to the baseline
# hazards from our "survival analysis" of impression number on the effect of
# clickicking on an impression. We use these numbers to draw vertical lines on
# the dotplots. The code we are using is:
#
#   abline(v = orig_hazard[1], col = 'gray', lty = 2)
#
# So, if we find that the click rate for one of the levels in (format1) is
# exceptionally larger than the baseline hazard, then that provides us with 
# some understanding that the difference is significant----that is, having 
# knowledge of format1 increases our ability to predict click thru rates.
#
# Similarly, using the baseline hazard for the 2nd impression, provides us with
# the logic for establishing an appropriate Ho (null hypothesis) for determining
# whether having knowledge of the unique paths from first impression to second
# impression improves our ability to predict the click thru rate for the 2nd
# impression.
orig_hazard <- c(`1` = 0.00606202379985191, `2` = 0.00375944012107966)


#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Some baseline feature engineering
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# For the first impression, we use the "format1". When we want to compare the
# proprotions for both first and second impressions, we combine the columns. For
# example, we create a column called "formats" to refer to the columns format1
# and format2 appended to each other; we do the same for "publisher1 and 2" and
# campaign1 and 2.

# paste the formats, campaigns, and publisher fields for first and 2nd
# impression into one column.
dat01[, formats     := paste(format1,    format2,    sep = " | ")]
dat01[, campaigns   := paste(campaign1,  campaign2,  sep = "\n")]
dat01[, publishers  := paste(publisher1, publisher2, sep = " | ")]

# Here, we convert the creative names to numbers so it is easier to read.
dat01$creativename1_1 <- as.numeric(as.factor(dat01$creativename1))
dat01$creativename2_2 <- as.numeric(as.factor(dat01$creativename2))
dat01[, creativenames := paste(creativename1_1, creativename2_2, sep = " | ")]


# all_res is an object that saves images and tables that then
# get put into reports.
all_res <- list()

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Definition of the output used in the analysis. 
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#-------------------------------------------------------------------------------
# Using dotplots in lieu of bar charts, as they seem to have the capability of 
# using the re-estate on the image canvas a bit better.  This set of code will
# produce dotcharts for the variables formats and formats
all_res[[length(all_res)+1]] = capturePlot({
  par(mfrow=c(1, 2))
  
  # Plot the first impression data
  rr1 = doplots(dat00, "format1"); 
  abline(v = orig_hazard[1], col = 'gray', lty = 2)
  
  # Plot the 2nd impression, conditional on first impression data
  rr2 = doplots(dat01, "formats");
  abline(v = orig_hazard[2], col = 'gray', lty = 2)
})

# Keep track of the computed tables. 
all_res[[length(all_res)+1]] <- inhouse_styling(rr1)
all_res[[length(all_res)+1]] <- inhouse_styling(rr2)

#-------------------------------------------------------------------------------
# This set of code will produce dotcharts for the variables campaign1 and
# campaigns.
all_res[[length(all_res)+1]] <- capturePlot({
  par(mfrow=c(1, 1))
  
  # Plot the first impression data
  rr1 <- doplots(dat00, "campaign1"); 
  abline(v = orig_hazard[1], col = 'gray', lty = 2)
})

all_res[[length(all_res)+1]] <- capturePlot({
  
    # Plot the 2nd impression, conditional on first impression data
    rr2 = doplots(dat01, "campaigns", addspace = TRUE); 
    abline(v = orig_hazard[2], col = 'gray', lty = 2)
})
  
all_res[[length(all_res)+1]] <- inhouse_styling(rr1)
all_res[[length(all_res)+1]] <- inhouse_styling(rr2)


#-------------------------------------------------------------------------------
# This set of code will produce dotcharts for the variables publisher1 and
# publishers.
all_res[[length(all_res)+1]] <- capturePlot({
  
  par(mfrow=c(1, 2))
  # Plot the first impression data.
  rr1 <- doplots(dat00, "publisher1");
  abline(v = orig_hazard[1], col = 'gray', lty = 2)
  
  # Plot the 2nd impression, conditional on first impression data
  rr2 <- doplots(dat01, "publishers"); 
  abline(v = orig_hazard[2], col = 'gray', lty = 2)
})

all_res[[length(all_res)+1]] <- inhouse_styling(rr1)
all_res[[length(all_res)+1]] <- inhouse_styling(rr2)

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# In this section, we peform a multivariate logistic regression to see if
# unique combinations of campaign1, format1, and publisher1 are predictive of 

# Note: we did't use this data in our analyses, but we want to book mark this
# for future investigation. 
res = glm(cbind(clicked, N-clicked) ~
               campaign1 + format1 + publisher1, 
             data = dat00[, .(clicked, N, campaign1, format1, publisher1)],
             family = "binomial"
)

# Save the regression coefficients.
rr = summary(res)
all_res[[length(all_res)+1]] <- inhouse_styling(rr$coefficients)

# Construct the data fields that we need for the dot plots.
dat00$p           <- predict(res, type = 'response')
dat00$logit       <- predict(res)
dat00$sefit       <- predict(res, se.fit = TRUE)$se.fit
dat00$logit_UL95  <- dat00$logit + dat00$sefit *  1.96
dat00$logit_LL95  <- dat00$logit - dat00$sefit *  1.96
dat00$UL95        <- exp(dat00$logit_UL95)/(1 + exp(dat00$logit_UL95))
dat00$LL95        <- exp(dat00$logit_LL95)/(1 + exp(dat00$logit_LL95))
rm(rr)


# Here, is where we convert factors to 'integers' so that we can can condense
# see the images a better. 
ff <- function(x) {
  if(is.character(x)) {
    return(as.factor(as.numeric(factor(x))))
  } else {
    return(x)
  }
}
dat00[] <- lapply(dat00, ff)
dat01[] <- lapply(dat01, ff)

dat00[, First_Impression_Multivariate_Analysis := 
        paste0(format1, " | ", campaign1, " | ", publisher1)]
all_res[[length(all_res)+1]] <- capturePlot({
  rr = doplots(dat00, "First_Impression_Multivariate_Analysis"); 
  abline(v = orig_hazard[1], col = 'black', lty = 2, lwd = 1)
})
all_res[[length(all_res)+1]] <- inhouse_styling(rr)

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# In this section, we peform a multivariate logistic regression to see if
# unique combinations of campaigns, formats, and publishers are predictive of 
# of clicks.
res = glm(cbind(clicked, N-clicked) ~ 
             campaigns + formats, 
             data =  dat01[, .(clicked, N, campaigns, formats, publishers)],
             family = "binomial"
)
rr <- summary(res)
all_res[[length(all_res)+1]] <- inhouse_styling(rr$coefficients)

dat01$p           <- predict(res, type = 'response')
dat01$logit       <- predict(res)
dat01$sefit       <- predict(res, se.fit = TRUE)$se.fit
dat01$logit_UL95  <- dat01$logit + dat01$sefit *  1.96
dat01$logit_LL95  <- dat01$logit - dat01$sefit *  1.96
dat01$UL95        <- exp(dat01$logit_UL95)/(1 + exp(dat01$logit_UL95))
dat01$LL95        <- exp(dat01$logit_LL95)/(1 + exp(dat01$logit_LL95))

dat01[, Two_Impressions_Multivariate_Analysis := paste0(formats, " | ", campaigns, " | ", publishers)]

all_res[[length(all_res)+1]] <- capturePlot({
  rr = doplots(dat01, "Two_Impressions_Multivariate_Analysis", cex = .8); 
  abline(v = orig_hazard[2], col = 'black', lty = 2, lwd = 1)
})

all_res[[length(all_res)+1]] <- inhouse_styling(rr)

params <- list()
params$all_res <-all.res <- all_res
params$set_title <- "Dot Plot Report"
render(
  paste0(prefix, 'TEMPLATES/missing_data_report2.Rmd'),
  output_file = paste0(prefix, 'dotplot_output2.html'),
  output_dir = paste0(prefix, 'OUTPUT/'),
  quiet = FALSE,
  params = params
)

file.show(paste0(prefix, 'OUTPUT/dotplot_output2.html'))

