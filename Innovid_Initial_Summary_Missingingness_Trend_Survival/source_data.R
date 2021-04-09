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

# This function will read the credentials from downloads folder. I believe it 
# loads the creds as a system variable, which is then passed transparently to
# "dbConnect" below.
use_credentials(file=AWS_keys_location)

con <- dbConnect(RAthena::athena(),
                 s3_staging_dir=s3_staging_dir,
                 region_name='us-east-2')

head(dbListTables(con, schema = "raw_consumer"))

origquery <- "select * from raw_consumer.innovid where cookieid in (
  select distinct(cookieid) as cookieid 
         from raw_consumer.innovid  where cookieid <> 'null'
         TABLESAMPLE BERNOULLI(.0010)
  );"

# origquery <- "select distinct(cookieid) 
#          from raw_consumer.innovid  where cookieid <> 'null'
#          limit 10
# ;"


origquery= "select 
  case when impressions > 0 then 1 else 0 end impressions,
  case when clickthroughs > 0 then 1 else 0 end as clickthroughs,
  case when completion25 > 0 then 1 else 0 end as completion25,
  case when completion50 > 0 then 1 else 0 end as completion50,
  case when completion75 > 0 then 1 else 0 end as completion75,
  case when completion100 > 0 then 1 else 0 end as completion100,
  count(1) as Count_of_Records
from raw_consumer.innovid
where lower(campaign) like '%trulicity%'
group by 1, 2, 3, 4, 5, 6
order by 1, 2, 3, 4, 5, 6
limit 1000"

(dat1      <- dbGetQuery(con, origquery))

fwrite(dat1, paste0(prefix, outputfile))


origquery= "select 
  case when impressions > 0 then 1 else 0 end impressions,
  case when clickthroughs > 0 then 1 else 0 end as clickthroughs,
  case when completion25 > 0 then 1 else 0 end as completion25,
  case when completion50 > 0 then 1 else 0 end as completion50,
  case when completion75 > 0 then 1 else 0 end as completion75,
  case when completion100 > 0 then 1 else 0 end as completion100,
  count(1) as Count_of_Records
from raw_consumer.innovid
where lower(campaign) like '%trulicity%'
group by 1, 2, 3, 4, 5, 6
order by 1, 2, 3, 4, 5, 6
limit 1000"

origquery= "select 
  impressions,
  clickthroughs,
  completion25,
  completion50,
  completion75,
  completion100
  from raw_consumer.innovid TABLESAMPLE BERNOULLI(.01) 
  where lower(campaign) like '%trulicity%'
  
"
#10/1000

#TABLESAMPLE BERNOULLI(.001) 
(dat0      <- dbGetQuery(con, origquery))

fwrite(dat0, paste0(prefix, outputfile))


dat1$p = dat1$Count_of_Records/sum(dat1$Count_of_Records)

sum(dat1$Count_of_Records)*.0001

dat1
x = dat0[, .N, .(impressions, clickthroughs, completion25, completion50, completion75, completion100)]
dat0[, sum(clickthroughs)]
dat0$rs = rowSums(dat0)


dat1$pp = c(
  dat0[rs==0, .N]/nrow(dat0),
  dat0[completion100>0, .N]/nrow(dat0),
  dat0[completion75>0, .N]/nrow(dat0),
  dat0[completion50>0, .N]/nrow(dat0),
  dat0[completion25>0, .N]/nrow(dat0),
  dat0[clickthroughs>0, .N]/nrow(dat0),
  dat0[impressions>0, .N]/nrow(dat0)
)
dat1$cnt = c(
  dat0[rs==0, .N],
  dat0[completion100>0, .N],
  dat0[completion75>0, .N],
  dat0[completion50>0, .N],
  dat0[completion25>0, .N],
  dat0[clickthroughs>0, .N],
  dat0[impressions>0, .N]
)

colSums(dat0>0)


tdat = dat0[, .(impressions, clickthroughs, completion25, completion50, completion75, completion100)]
tdat[] = lapply(tdat, function(x) { return(x > 0);})
setDT(tdat)
colSums(tdat)
tdat[, colSums(.SD)]*1/.0001
dat1
tmp <- function(x) { 
  mm <- colSums(x)
  list(sums=mm)
}
tdat[, tmp(.SD)]
tdat[, group := rbinom(.N, 1, .4)]

dat1
origquery= "select 
  impressions,
  clickthroughs,
  completion25,
  completion50,
  completion75,
  completion100
  from raw_consumer.innovid TABLESAMPLE BERNOULLI(.01) 
  where lower(campaign) like '%trulicity%'
"
(dat2      <- dbGetQuery(con, origquery))

################################
origquery= "select 
  channelname,
  case when impressions > 0 then 1 else 0 end impressions,
  case when clickthroughs > 0 then 1 else 0 end as clickthroughs,
  case when completion25 > 0 then 1 else 0 end as completion25,
  case when completion50 > 0 then 1 else 0 end as completion50,
  case when completion75 > 0 then 1 else 0 end as completion75,
  case when completion100 > 0 then 1 else 0 end as completion100,
  count(1) as Count_of_Records
from raw_consumer.innovid
where lower(campaign) like '%trulicity%'
group by 1, 2, 3, 4, 5, 6, 7
order by 1, 2, 3, 4, 5, 6,7"

#10/1000
#TABLESAMPLE BERNOULLI(.001) 
(dat2      <- dbGetQuery(con, origquery))

fwrite(dat2, paste0(prefix, "geo_output.csv"))

dat2$geo <- gsub("^.+,", "", dat2$channelname)
dat2$geo <- gsub(")", "", dat2$geo)
cbind(table(dat2$geo))
dat2 <- dat2[geo != "OLV_Diabetes-c-Trulicity_Diabetes Type 2_Sharecare_END_CD_NA_CONT_T2D Watch_PRE_:60_CPCV_INVD_VPAID_GM_ENG_COMPCT"]

tmp2 <- function(x) { 
  mm <- colSums(x)
  list(sums=mm)
}

tmp = dat2[, .(impressions, clickthroughs, completion25, completion50, completion75, completion100, Count_of_Records, geo)]
tmp[, colSums(.SD), geo]


t1 =  tmp[impressions   == 1, .(sum(Count_of_Records)), geo]
t2 =  tmp[clickthroughs == 1, .(sum(Count_of_Records)), geo]
t3 =  tmp[completion25  == 1, .(sum(Count_of_Records)), geo]
t4 =  tmp[completion50  == 1, .(sum(Count_of_Records)), geo]
t5 =  tmp[completion75  == 1, .(sum(Count_of_Records)), geo]
t6 =  tmp[completion100 == 1, .(sum(Count_of_Records)), geo]

allt = merge(t1,   t2, by = 'geo', all = TRUE); setnames(allt, c("V1.x", "V1.y"), c("impressions", "clickthroughs"))
allt = merge(allt, t3, by = 'geo', all = TRUE); setnames(allt, c("V1"), "completion25")
allt = merge(allt, t4, by = 'geo', all = TRUE); setnames(allt, c("V1"), "completion50")
allt = merge(allt, t5, by = 'geo', all = TRUE); setnames(allt, c("V1"), "completion75")
allt = merge(allt, t6, by = 'geo', all = TRUE); setnames(allt, c("V1"), "completion100")

