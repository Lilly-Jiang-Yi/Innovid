rm(list=ls())
gc(reset = TRUE) 

library(survival)
library(data.table)
library(RPostgres)
library(DBI)
library("bit64")


# This code plots survival curves of waiting times
# Note: data source is for the redshift tables.
source(paste0("FUNCTIONS/Functions.R"))


source("~./config.R")


con = get_con(db_user = db_user, db_password = db_password)

query = "select cookieid,
  datetime_imp1, 
  datetime_imp2, 
  date_diff('second', datetime_imp1, datetime_imp2) as tm1,
  datetime_imp3,
  datetime_imp4
  from
  ca.yj_invd_first5_imp 
where random() < 0.01"

dat = get_data(con, query)


class(dat$datetime_imp2)
tt = dat[, .(datetime_imp1, datetime_imp2, tm1)]
tt[,                     status := 1]
tt[is.na(datetime_imp2), status := 0]
MX = max(tt$datetime_imp2,na.rm =TRUE)
tt[is.na(datetime_imp2), datetime_imp2 := MX]

tt[, timdiff1 := as.numeric(difftime(datetime_imp2, datetime_imp1, units = 'secs'))]
is.numeric(tt$timdiff1)

options(scipen = 9999)
x11()
par(mfrow=c(1, 2))
plot(survfit(Surv(timdiff1 , status) ~1, data = tt),
     lwd = 3,
     xlim =c(0, 600),
     main  = 'Time Between First Two Impressions\nZoomed Into x-axis',
     xlab = 'Time (Seconds)',
     ylab = 'Survival'
)
grid()
abline(v = 60, lwd = 2, col= 'green', lty = 2)
abline(v = 60*2, lwd = 2, col= 'blue', lty = 2)
abline(v = 60*3, lwd = 2, col= 'magenta', lty = 2)
legend('bottomright',
  c('1st Minute', '2nd Minute', '3rd Minute'),
  fill = c('green', 'blue', 'magenta'),
  bg = 'white'
)
grid()


tt[, timdiff2 := as.numeric(difftime(datetime_imp2, datetime_imp1, units = 'days'))]
plot(survfit(Surv(timdiff2 , status) ~1, data = tt),
     lwd = 3,
     main  = 'Time Between First Two Impressions\nEntire Waiting Time',
     xlab = 'Time (Days)',
     ylab = 'Survival'
)
grid()

