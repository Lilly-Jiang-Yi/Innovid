rm(list=ls())
gc(reset = TRUE)
options(scipen=999)

library(rmarkdown)
library(data.table)
library(kableExtra)
library(R.devices)
library(DBI)
library(RPostgres)
library(qualvar)
library(bit64)

prefix = "REPORTS_loglevel/"

# config file. Simply defines "db_user" and "db_password"
#source('~/config.R')

# In-house functions.
source(paste0(prefix, "FUNCTIONS/Functions.R"))

# # Get Connection string from 
# connstr = get_con(db_user, db_password)

#-----------------------------------------------------------------------
# Find tables in redshift. 
# query = "SELECT * FROM pg_catalog.pg_tables"
# (dat <- get_data(connstr, query) )
# 
# dat[grep("innovid", dat$tablename),]
# innovid_summary_data
# innovid_summary_data_stg

#query = "select count(*) from ngca.trulicity.innovid_summary_data"
#(dat <- get_data(connstr, query) )


# query = "select * from ngca.trulicity.innovid_summary_data"
# (dat <- get_data(connstr, query) )

dat <- fread(paste0(prefix, 'DAT/sample_of_data.csv'))

ids <- sapply(dat, is.numeric)
ids <- ids[which(ids == FALSE)]
nms <- names(ids)

dat   <- dat[, ..nms]
dat[] <- lapply(dat, function(x) { if(is.character(x)) {x <- as.factor(x)}; return(x) })

# If a varible is classified as date, the convert to factor
dat[] <- lapply(dat, function(x) { if("Date" %in% class(x)) {x <- as.factor(x)}; return(x) })


#-----------------------------------------------

total_levels_w_NA  <- lapply(dat[, ..nms], function(x) { x = nlevels(x) + any(is.na(x)); x <- as.table(x); names(x) <- "Nlevels"; x})
qual_var_w_NA      <- lapply(dat[, ..nms], function(x) { round(DM(table(x, useNA = "ifany")), 4)})

total_levels_wo_NA <- lapply(dat[, ..nms], function(x) { x = nlevels(x) ; x <- as.table(x); names(x) <- "Nlevels"; x})
qual_var_wo_NA     <- lapply(dat[, ..nms], function(x) { round(DM(table(x)), 4)})

qual_var_wo_NA[] <- lapply(qual_var_wo_NA, function(x) {if(is.nan(x)){x = 0}; return(x)})
qual_var_w_NA[] <- lapply(qual_var_w_NA, function(x) {if(is.nan(x)){x = 0}; return(x)})


convert_table <- function(x) {
  x        <- table(x)
  fx       <- names(x)
  names(x) <- "Variable"
  x[1]     <- fx
  x        <- as.table(x)
  return(x)
}
convert_table2 <- function(x) {
  if(is.nan(x)) {x = 1}
  x        <- table(x)
  fx       <- names(x)
  names(x) <- "IQV"
  x[1]     <- fx
  x        <- as.table(x)
  return(x)
}
all_res <- list()



all_res[[length(all_res) + 1]] <- list(preamble = 
      "This document summarizes the Index of Qualitative Variation. 
      
      The IQV is a measure of variation for a categorical variable. This is anagolous to a standard 
      deviation for a numeric data type (such as, age, income, etc...).
      
      Briefly, speaking when the IQV is equal to zero, that is an indication that there is no 
      variability observed in your variable. Strictly, speaking it would imply that there is only
      one value in your fied. Imagine a data field that called 'Gender' but it was filled with only
      'male' (or vice versa, 'female').
      
      When the IQV equals one. It means that the frequency of observations are perfectly allocated
      proportionately to each group. For example, a column called 'Gender' that has exactly 50% men
      and 50% women. 
      
      The advantage of the IQV score is that it provides a value for summarizing fields with many 
      possible levels, such as 'landing_page', or 'url', and so on. 
      
      The IQV can be used to prioritize your effort to explore those variables that might yield more
      value in your model building/exploratory effort.
      "
)



all_res[[length(all_res) + 1]] <- list(img = R.devices::capturePlot({
  par(mfrow=c(1, 2))
  hist(unlist(qual_var_w_NA),
       main = 'Distribution of IQV (with NAs)',
       col  = 'steelblue',
       xlab = 'IQV'
  )
  grid()  

  plot(ecdf(unlist(qual_var_w_NA)),
       main = 'ECDF of IQV (With NAs)',
       col  = 'steelblue',
       ylab = 'Cumulative Probability',
       xlab = 'IQV',
       verticals = TRUE,
       cex = .2,
       lwd = 3
  )
  grid()  
}))

all_res[[length(all_res) + 1]] <- list(img = R.devices::capturePlot({
  par(mfrow=c(1, 2))
  hist(unlist(qual_var_wo_NA),
       main = 'Distribution of IQV (Without NAs)',
       col  = 'steelblue',
       xlab = 'IQV'
  )
  grid()  
  
  plot(ecdf(unlist(qual_var_wo_NA)),
       main = 'ECDF of IQV (Without NAs)',
       col  = 'steelblue',
       ylab = 'Cumulative Probability',
       xlab = 'IQV',
       verticals = TRUE,
       cex = .2,
       lwd = 3
  )
  grid()  
}))

all_res[[length(all_res) + 1]] <- list(separator = "Numerical Summaries")
  
mat <- matrix(0, length(nms), 5)
for(i in 1:length(nms)) {
  mat[i,] <- t(
    c(
      convert_table(nms[i]),
      total_levels_w_NA[[i]],
      convert_table2(round(qual_var_w_NA[[i]],4)),
      total_levels_wo_NA[[i]],
      convert_table2(round(qual_var_wo_NA[[i]],4))
    )
  )
    
}


mat <- as.data.table(mat)
setnames(mat, c("V1", "V2", "V3", "V4", "V5"), 
         c("Variable Name", "Nlevels (including NAs)", "IQV (With NAs)",
           "Nlevels w/o NAs", "IQV (w/o) NAs"))

setorder(mat, -`IQV (With NAs)`)

inhouse_styling <- function(dat) {
  kable(dat, digits = 4) %>% kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive"))
}

all_res[[length(all_res)+1]] = list(tab = inhouse_styling(mat))

params <- list()
params$all_res <- all_res
params$set_title <- "Index of Qual Variation"
render(
  paste0(prefix, 'TEMPLATES/image_template_wider_img.Rmd'),
  output_file = paste0(prefix, 'IQV_report.html'),
  output_dir = paste0(prefix, 'OUTPUT/'),
  quiet = FALSE,
  params = params
)

file.show(paste0(prefix, 'OUTPUT/IQV_report.html'))

