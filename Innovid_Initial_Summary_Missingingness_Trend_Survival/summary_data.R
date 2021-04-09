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
library(summarytools)

reticulate::use_condaenv()

prefix  <- "./"

source(paste0(prefix, 'standard_config.R'))

dat <- fread(paste0(prefix, 'DAT/sample_of_data.csv'))


#dat[] <- lapply(dat, as.character)
view(dfSummary(dat),file = paste0(prefix, "OUTPUT/initial_summary_of_data.html"))
file.show(paste0(prefix, "OUTPUT/initial_summary_of_data.html"))




