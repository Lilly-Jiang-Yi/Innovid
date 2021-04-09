rm(list=ls())
gc(reset = TRUE)
options(scipen = 999)


# The purpose of this code is to compute the hazard and 'survival' curve for
# the effect of receiving an impression and the first click. 
# 
# We use kaplain-Meier fits (and discretized survival models) to compute these
# hazards. While impression count is not a survival analysis (in the sense that 
# we are using time), it is a useful analogues for our analysis. 
#

library(rmarkdown)
library(data.table)
library(kableExtra)
library(callr)
library(R.devices)
library(DBI)
library(RPostgres)
library(DBI)
library(aws.signature)
library("bit64")
library(RAthena)
library(reticulate)
library(survival)
library(discSurv)

reticulate::use_condaenv()

download_data = FALSE

prefix  <- "./"

source(paste0(prefix, 'standard_config.R'))

source("Functions/Functions_unique_path.R")
source("FUNCTIONS/Functions.R")

# This function will read the credentials from downloads folder. I believe it 
# loads the creds as a system variable, which is then passed transparently to
# "dbConnect" below.
use_credentials(file=AWS_keys_location)

# con <- dbConnect(RAthena::athena(),
#                  s3_staging_dir=s3_staging_dir,
#                  region_name='us-east-2'
# )
# 
# 
# query = "
# select format, publisher, campaign, count(*) 
#   from raw_consumer.yj_invd_imp_order 
# where order_imp = 1
# group by format, publisher, campaign
# "
# (tmptable      <- dbGetQuery(con, query))
# inhouse_styling(tmptable)
# setnames(tmptable, "_col3", "N")
# inhouse_styling(tmptable[, sum(N), format])



# This function contains the query to download data from Yi's temporary 
# table in Athna. This code will take a random sample of the data. If you find
# that the nubmer of observations are not large enough, you can re-run this 
# function call, and the code will pull another random sample and to the
# existing data.
#
# Also, when run with download_data = FALSE, it will de-dupe those records that 
# may have been pulled from a previous call.
dat <- get_unique_path_data(download_data)

format(object.size(dat), units = 'Mb')
dat$cookieid <- as.integer(dat$cookieid)
format(object.size(dat), units = 'Gb')

# All_res is a list. This list will contain all objects for our
# markdown reports. 
all_res <- list()
all_res[[length(all_res)+1]] <- capturePlot({
  par(mfrow=c(1, 2))
  hist(dat[, .N, cookieid]$N, main = 'Histogram of Counts', col = 'steelblue')
  grid()
  plot(ecdf(dat[, .N, cookieid]$N),
       main = 'ECDF of Counts',
       xlab = 'Count', 
       ylab = 'Cumulative Proportion',
       verticals = TRUE,
       cex = .2,
       lwd= 3
  )
  grid()
})
nrow(dat)
###########################################
# Feature Engineering section
###########################################
recode_data_by_reference(dat);

# Dat surv---we remove all rows. There should be one row per customer 
# here. The first part of this snippet will extract those who had yet
# to click on an impression (i.e., "dat[firstclick == 0 & ord == mxord,]"
# The second part will extract the very last click.
dat_surv <- rbind(
  dat[firstclick == 0 & ord == mxord,],
  dat[ord == firstclick]
)

# Create a new longform data structure. And use only those variables
# that we actually need.

# Define the dependent variable and determine whether it is 
# censored or not. Let status = 0 for the situation where 
# we have where it is censored. Let status = 1 when a click
# was observed.
dat_surv[firstclick == 0, y      := mxord]
dat_surv[firstclick  > 0, y      := firstclick]
dat_surv[firstclick == 0, status := 0]
dat_surv[firstclick  > 0, status := 1]


dat_longform <- dat_surv[, .(y, status) ]

# "dataLong" can only be used on data.frames (not data.tables). We could 
# restructure this manually pretty easily, but want to leverage discSurv's error
# checking code for good measure
setDF(dat_longform)
dat_longform = dataLong(dat_longform, "y", "status", timeAsFactor = TRUE)
setDT(dat_longform)

# Count all unique rows. In order to make glm run faster, we roll up the data
# and pass an argument to "weights"
dat_longform_tmp <- dat_longform[, .N, .(timeInt, y)]

res <- glm(y ~ timeInt, 
           data    = dat_longform_tmp, 
           family  = 'binomial', 
           weights = N)
summary(res)


# Define mx, the maximum number of iterations to display in the plot.
# Intervention defines when we define the media campaign intervention.
mx           <- 19
intervention <- 5
ndat         <- data.frame(timeInt = as.factor(1:mx))

# Estimate the hazard.
hazard <- predict(res, 
                  newdata = ndat, 
                  type = 'response'
)

dput(hazard)


all_res[[length(all_res)+1]] <- capturePlot({
  par(mfrow =c(1, 2))
  plot(survfit(Surv(y, status)~1, data = dat_surv),
       xlim = c(0, mx),
       xlab = 'Impression #',
       ylab = 'Survival',
       main = 'KM Curve by Impression #',
       ylim = c(.8, 1)
  )
  points(0:mx, c(1, estSurv(hazard)), col = 'red', pch = 19)
  
  grid()
  legend('bottomleft',
         'Fit',
         fill = 'red',
         bg = 'white'
  )
  
  plot(hazard,
       ylab = 'Predicted Probability',
       xlab = 'Impression #',
       main = "Predicted Prob. of Clicking by Impression Number",
       pch  = 19
  )
  grid()
})


rr2 = summary(res)
all_res[[length(all_res)+1]] <- inhouse_styling(rr2$coefficients)

rm(dat_longform, dat_longform_tmp)
gc(reset = TRUE)

# ##############################################################################
# # This section of code is designed to model the effect of interventions
# # on the prediction.
# 
# # Intervention defines when we define the media campaign intervention.
# mx           <- 19
# intervention <- 5
# 
# dat[, mxfirstclick := max(firstclick), cookieid]
# # 
# # # Create a new data structure: "dat longform for prediction"
# dat_lf4_pred <- rbind(
#   dat[firstclick == 0 & mxfirstclick == 0],
#   dat[ord <= mxfirstclick]
# )
# 
# # Save the variables that we need for the prediction.
# dat_lf4_pred <- dat_lf4_pred[, .(
#   ord,      firstclick, mxfirstclick, cookieid,
#   campaign, format,     publisher,    duration
# )]
# 
# # Defining the dependent varaible and status. 
# # Status = 0 is censored
# # Status = 1 means an event occurred.
# dat_lf4_pred[,                  y            := ord]
# dat_lf4_pred[firstclick == 0,   status       := 0]
# dat_lf4_pred[ord == firstclick, status       := 1]
# #dat_lf4_pred[,                  ord          := NULL]
# dat_lf4_pred[,                  firstclick   := NULL]
# dat_lf4_pred[,                  mxfirstclick := NULL]
# dat_lf4_pred$status = as.integer(dat_lf4_pred$status)
# dat_lf4_pred$y      = as.integer(dat_lf4_pred$y)
# # THis can take a long time to compute. We should parallelize this function
# # in the future.
# #dat_lf4_pred <- copy(dat_surv)
# # dat_lf4_pred <- dat[ , .(
# #   cookieid, campaign, format, publisher,
# #   duration, status,   y
# #   )
# # ]
# 
# dat_lf4_pred <- dat_lf4_pred[, .(
#   cookieid, y, status, format
# )]
# 
# 
# table(dat_lf4_pred$status)
# setDF(dat_lf4_pred)
# #dat_lf4_pred <- dat_lf4_pred[1:1000,]
# #dat_lf4_pred$status = rbinom(nrow(dat_lf4_pred), 1, .4)
# 
# # cooks <- unique(dat_lf4_pred$cookieid)
# # 
# # backgrounddisck <- function(x) {
# # 
# #   x = discSurv::dataLongTimeDep(x,
# #                       "y",
# #                       "status",
# #                       "cookieid",
# #                       timeAsFactor = TRUE
# #   )
# #   return(x)
# # }
# # nsplits = 6
# # ids = split(1:length(cooks), sort(1:length(cooks) %% nsplits))
# # 
# # #length(cooks[ids[[1]]]) == length(cooks[ids[[2]]])
# # 
# # allvals = list()
# # for(i in 1:nsplits) {
# #   idx <- which(dat_lf4_pred$cookieid %in% cooks[ids[[i]]])
# #   allvals[[i]] = r_bg(backgrounddisck,list(x = dat_lf4_pred[idx,]))
# # }
# # 
# # system.time({
# #   for(i in 1:nsplits) {
# #     allvals[[i]]$wait()
# #   }
# # })
# # newvals <-list()
# # 
# # for(i in 1:nsplits) {
# #   newvals[[i]] <- allvals[[i]]$get_result()
# # }
# # table(newvals[[1]]$timeInt)
# # rm(allvals)
# # #rm(newvals)
# # gc(reset = TRUE)
# # 
# # dat_lf4_pred_tmp = rbindlist(newvals)
# # 
# # dim(dat_lf4_pred_tmp)
# 
# sss_time <- system.time({
# dat_lf4_pred_tmp2 = dataLongTimeDep(dat_lf4_pred,
#                                    "y",
#                                    "status",
#                                    "cookieid",
#                                    timeAsFactor = TRUE
# )
# })
# sss_time
# 
# 
# dim(dat_lf4_pred_tmp)
# dim(dat_lf4_pred_tmp2)
# dim(dat_lf4_pred)
# 
# names(dat_lf4_pred)
# setDT(dat_lf4_pred_tmp)
# setDT(dat_lf4_pred_tmp2)
# setDT(dat_lf4_pred)
# 
# 
# 
# tt1 <- dat_lf4_pred_tmp[, .N,  .(timeInt, y, campaign, format,publisher )]
# tt2 <- dat_lf4_pred_tmp2[, .N, .(timeInt, y, campaign, format,publisher )]
# 
# dat_lf4_pred$y <- NULL
# setDT(dat_lf4_pred)
# setnames(dat_lf4_pred, c("status", "ord"), c("y", "timeInt"))
# 
# dat_lf4_pred$timeInt <- as.factor(dat_lf4_pred$timeInt)
# tt3 <- dat_lf4_pred[,      .N, .(timeInt, y, campaign, format,publisher )]
# table(dat_lf4_pred_tmp$status)
# tt3$timeInt <- as.factor(tt3$timeInt)
# table(dat_lf4_pred$y)
# table(dat_lf4_pred$y)
# identical(tt1, tt2)
# identical(tt1, tt3)
# nrow(tt1)
# nrow(tt2)
# nrow(tt3)
# setorder(tt1, N, timeInt, y, campaign, format,publisher)
# setorder(tt3, N, timeInt, y, campaign, format,publisher)
# 
# names(tt1)
# cbind(tt1$timeInt, tt3$timeInt)
# identical(tt1$timeInt, tt3$timeInt)
# identical(tt1$campaign, tt3$campaign)
# table(tt1$y, tt3$y)
# table(tt1$y,useNA = 'always')
# plot(tt1$N, tt3$N)
# 
# setDT(dat_lf4_pred_tmp)
# 
# #table(dat_lf4_pred_tmp$timeInt)
# #table(dat_lf4_pred_tmp$y[1:10000])
# #is.integer(dat_lf4_pred)
# 
# 
#  
# # Create a rolled up data structure so glm computes faster.
# dat_lf4_pred_tmp2 <- dat_lf4_pred_tmp2[, .N, .(y, timeInt, format)]
# res2              <- glm(y ~ timeInt ,
#                          data    = dat_lf4_pred_tmp2,
#                          family  = 'binomial',
#                          weights = N
# )
# 
# res3              <- glm(y ~ timeInt * format,
#                          data    = dat_lf4_pred_tmp2,
#                          family  = 'binomial',
#                          weights = N
# )
# summary(res3)
# anova(res2, res3,test = "LRT")
# # 
# # # Save the coefficients into an object.
# # rr = summary(res2)
# # all_res[[length(all_res)+1]] <- inhouse_styling(rr$coefficients)
# # 
# # rr2 = summary(res3)
# # all_res[[length(all_res)+1]] <- inhouse_styling(rr2$coefficients)
# 
# 
# # ######==========================
# #x11()

plot_estimates <- function(mod_obj, mx, intervention) {

  # Assuming that someone has been given ad canvas 
  ndat_canvas  <- data.frame(timeInt = as.factor(1:mx), format = "Ad Canvas")
  
  # Assuming that someone has been given pre-roll.
  ndat_preroll <- data.frame(timeInt = as.factor(1:mx), format = "Pre Roll")

  # Assuming that someone has been given an intervention
  ndat_preroll_to_canvas <- data.frame(
    timeInt = as.factor(1:mx),
    format = c(
      rep("Pre Roll",intervention),
      rep("Ad Canvas", mx - intervention)
    )
  )
  
  canvas_hazard <- predict(mod_obj,
                    newdata = ndat_canvas,
                    type = 'response'
  )
  
  plot(0:mx, c(1, estSurv(canvas_hazard)), 
       type = 'l',
       lwd = 3,
       xlab= 'Impression Number',
       ylab = 'Survival',
       main = 'Estimating Change in Media Format',
       ylim = c(.95, 1)
  )
  
  prerool_hazard <- predict(mod_obj,
                    newdata = ndat_preroll,
                    type = 'response'
  )
  lines(0:mx, c(1, estSurv(prerool_hazard)), 
        type = 'l', 
        col = 'red',
        lwd = 3
  )
  
  
  # Create a data.frame for modeling change from pre roll to ad canvas.
  
  inter_hazard <- predict(mod_obj,
                    newdata = ndat_preroll_to_canvas,
                    type = 'response'
  )

  lines(0:mx, c(1, estSurv(inter_hazard)),
        type = 'l',
        col = 'green',
        lwd = 3
  )
  grid()
  legend('topright',
         c('Ad Canvas', 'Pre Roll', 'Ad Canvas to Pre Roll'),
         fill = c('black', 'red', 'green'),
         bg = 'white')

  return(
    rbind(
      canvas_hazard,
      prerool_hazard,
      inter_hazard
    )
  )
 
}

#######################################################
# time between events.

ttime = function(dat, order1, order2) {
  
  tdat <- merge(dat[ord == order1, .(cookieid, datetime_imp2)], 
                dat[ord == order2, .(cookieid, datetime_imp2)],
                by = 'cookieid',
                all.x = TRUE
  )
  tdat[,                       status := 1]
  tdat[is.na(datetime_imp2.y), status := 0]
  mxy = max(tdat$datetime_imp2.y, na.rm = TRUE)
  tdat[is.na(datetime_imp2.y), datetime_imp2.y := mxy]
  tdat[, y := difftime(datetime_imp2.y, datetime_imp2.x, units = 'secs')]
  tdat[, y := as.numeric(y)]
  return(tdat)
}
dat[, datetime_imp2 := datetime_imp]
dat[, datetime_imp2 := gsub("T", " ", datetime_imp2)]
dat[, datetime_imp2 := gsub("Z", "", datetime_imp2)]
dat[, datetime_imp2:= as.POSIXct(datetime_imp2)]
dat[1, datetime_imp2]
time12 <- ttime(dat, 1, 2)
time23 <- ttime(dat, 2, 3)
time34 <- ttime(dat, 3, 4)
time45 <- ttime(dat, 4, 5)

time12[1, .(datetime_imp2.x, datetime_imp2.y, y, status)]
time12[4, .(datetime_imp2.x, datetime_imp2.y, y, status )]
class(time12$datetime_imp2.x)



all_res[[length(all_res)+1]] <- capturePlot({
  plot(survfit(Surv(y, status)~1, data=time12),
       main = 'Time Between Impressions',
       xlab = 'Time (in Seconds)',
       ylab = 'Proportion',
       lwd = 3
       #,xlim = c(0, 60*60*24)
  )
  lines(survfit(Surv(y, status)~1, data=time23), col = 'red', lwd = 3)
  lines(survfit(Surv(y, status)~1, data=time34), col = 'blue', lwd = 3)
  lines(survfit(Surv(y, status)~1, data=time45), col = 'green', lwd = 3)
  grid()
  legend('topright',
         c('Time Between Impression 1 and 2',
           'Time Between Impression 2 and 3',
           'Time Between Impression 3 and 4',
           'Time Between Impression 4 and 5'),
         fill = c('black', 'red', 'blue', 'green'),
         bg = 'white'
  )
})


params <- list()
params$all_res <-all.res <- all_res
params$set_title <- "Survival Curve Report: Time to First Click"
render(
  paste0(prefix, 'TEMPLATES/missing_data_report_wider_img.Rmd'),
  output_file = paste0(prefix, 'survival_report.html'),
  output_dir = paste0(prefix, 'OUTPUT/'),
  quiet = FALSE,
  params = params
)

file.show(paste0(prefix, 'OUTPUT/survival_report.html'))

