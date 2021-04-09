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
library(stringr)

reticulate::use_condaenv()

prefix  <- "./"

source(paste0(prefix, 'standard_config.R'))
source(paste0(prefix, 'FUNCTIONS/Functions.R'))

################################
origquery= "select 
  channelname,
    sum(impressions) as impressions,
    sum(clickthroughs) as clickthroughs,
    sum(completion25) as completion25,
    sum(completion50) as completion50,
    sum(completion75) as completion75,
    sum(completion100) as completion100
from raw_consumer.innovid
where lower(campaign) like '%trulicity%'
group by channelname"


# use_credentials(file=AWS_keys_location)  
# con <- dbConnect(RAthena::athena(),
#                  s3_staging_dir=s3_staging_dir,
#                  region_name='us-east-2')
# 
# 
# (dat2      <- dbGetQuery(con, origquery))
# 
# fwrite(dat2, paste0(prefix, "DAT/geo_output.csv"))
dat2 <- fread(paste0(prefix, "DAT/geo_output.csv"))

all_res <- list()

all_res[[length(all_res)+1]] <- list(
preamble =
"Summary of unique City/State names:

Unit of Analysis: row/log level (no attempt was made to roll up to cookie id)

Note: some hard coding was done so that we could roll up the states into
names that made sense. Some cities across State lines were combined in the
data. In those situations, we interpolated the data (dividing frequencies by 2).

Also, there was one state/name combination that was clearly not a state, and
is probably an import error. This was the following:

'OLV_Diabetes-c-Trulicity_Diabetes Type 2_Sharecare_END_CD_NA_CONT_T2D Watch_PRE_:60_CPCV_INVD_VPAID_GM_ENG_COMPCT'

Also, we have not data for the states of DE or NJ. This may explain the problem
with the above.

"

)

# Visually inspect the table.list(tab = inhouse_styling(mat))
all_res[[length(all_res)+1]] <- list(tab = inhouse_styling(cbind(unique(dat2$channelname))))

# Drop a channel name that was clearly an import error.
dat2 <- dat2[channelname != "OLV_Diabetes-c-Trulicity_Diabetes Type 2_Sharecare_END_CD_NA_CONT_T2D Watch_PRE_:60_CPCV_INVD_VPAID_GM_ENG_COMPCT"]

# Hard code some of the field names so we can automate processing. 
# In short, we separate states with the hyphen as a delimeter. Because
# some cities have hyphens in them, we want to hard code those names.
dat2[channelname == "Rochester-Austin, MN-Mason City, IA", channelname := "Rochester, MN-Mason City, IA"]
dat2[channelname == "Davenport,IA-Rock Island-Moline,IL",  channelname := "Davenport, IA-Moline, IL" ]
dat2[channelname == "Quincy, IL-Hannibal, MO-Keokuk, IA",  channelname := "Quincy, IL-Hannibal, MOKeokuk, IA" ]
dat2[channelname == "Washington, DC (Hagerstown, MD)",     channelname := "Washington, DC-Hagerstown, MD" ]
dat2[channelname == "Wichita Falls, TX And Lawton, OK",    channelname := "Wichita Falls, TX-Lawton, OK" ]
dat2[channelname == "Champaign And Springfield-Decatur,IL",channelname := "Champaign, IL" ]
dat2[channelname == "Norfolk-Portsmouth-Newport News,VA",  channelname := "Norfolk, VA"]
dat2[channelname == "Tri-Cities, TN-VA",                   channelname := "somename, TN-somename, VA"]


# Find all cities/states
nms <- unique(dat2$channelname)

# Some cities were lumped together. This is somewhat of a problem.
# if the cities fall into separate states. We finess this problem
# by interpolating (i.e., splitting the counts) into the
# two states: "Boston, MA-Manchester, NH"
nms    <- nms[which(str_count(nms, ',')>1)]
newnms <- strsplit(nms, '-')

# For those records that recorded counts in different states, we split the
# Count_of_Records/2.
split_diff <- function(dat2, nms1, newnms1){
  tmp = dat2[channelname == nms1]
  #tmp[, Count_of_Records := (Count_of_Records/2)]
  tmp[, impressions   := impressions   / 2] 
  tmp[, clickthroughs := clickthroughs / 2]
  tmp[, completion25  := completion25  / 2]
  tmp[, completion50  := completion50  / 2]
  
  tmp1 = copy(tmp)
  tmp2 = copy(tmp)
  tmp1[, channelname := newnms1[1]]
  tmp2[, channelname := newnms1[2]]
  rbind(tmp1, tmp2)
}
split_dat2 = list()
for(i in 1:length(nms)) {
  split_dat2[[length(split_dat2)+1]] <- split_diff(dat2, nms[i], newnms[[i]])
}
split_dat2 <- rbindlist(split_dat2)

"%ni%" <- Negate("%in%")
dat3 <- dat2[channelname %ni% nms]
dat3 <- rbind(dat3, split_dat2)

# Make sure the counts add up right. If this does not equal to zero, then 
# we split the states wrong.
sum(dat3$Count_of_Records) - sum(dat2$Count_of_Records)

dat3$geo <- gsub("^.+, ", "", dat3$channelname)
dat3$geo <- gsub(" ", "", dat3$geo)
cbind(table(dat3$geo))


setnames(dat3, 'geo', 'state')
dat3[grep('DC', state)]
dat3[grep('RI', state)]
dat3[grep('VT', state)]

dat3[grep('NJ', state)]
dat3[grep('DE', state)]

tmp2 <- function(x) { 
  mm <- colSums(x)
  list(sums=mm)
}
nms = names(dat3)
allt = dat3[, .(sum(impressions), sum(clickthroughs), sum(completion25), sum(completion50), sum(completion75), sum(completion100)), state]
names(allt) <- c("state",  "impressions",
                 "clickthroughs", "completion25",  "completion50",
                 "completion75",  "completion100")

# #tmp[, colSums(.SD), state]
# 
# t1 =  tmp[impressions   == 1, .(sum(Count_of_Records)), state]
# t2 =  tmp[clickthroughs == 1, .(sum(Count_of_Records)), state]
# t3 =  tmp[completion25  == 1, .(sum(Count_of_Records)), state]
# t4 =  tmp[completion50  == 1, .(sum(Count_of_Records)), state]
# t5 =  tmp[completion75  == 1, .(sum(Count_of_Records)), state]
# t6 =  tmp[completion100 == 1, .(sum(Count_of_Records)), state]
# 
# allt = merge(t1,   t2, by = 'state', all = TRUE); setnames(allt, c("V1.x", "V1.y"), c("impressions", "clickthroughs"))
# allt = merge(allt, t3, by = 'state', all = TRUE); setnames(allt, c("V1"), "completion25")
# allt = merge(allt, t4, by = 'state', all = TRUE); setnames(allt, c("V1"), "completion50")
# allt = merge(allt, t5, by = 'state', all = TRUE); setnames(allt, c("V1"), "completion75")
# allt = merge(allt, t6, by = 'state', all = TRUE); setnames(allt, c("V1"), "completion100")

all_res[[length(all_res)+1]] <- list(
  preamble =
    "Summary of roll up"
)

setorder(allt, -impressions)
all_res[[length(all_res) +1]] <- list(tab = inhouse_styling(allt))


library(usmap)
library(ggplot2)
#setnames(allt, "geo", "state")

all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  hist(allt$impressions,
       main = 'Histogram of Impressions by State',
       xlab= 'Number of Impressions',
       ylab = 'Frequency',
       col = 'steelblue'
  )
  grid()
}))

all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  plot(ecdf(allt$impressions),
       main = 'ECDF of Impressions by State',
       xlab= 'Number of Impressions',
       ylab = 'Cumulative Proportion',
       col = 'steelblue',
       lwd = 3,
       cex = .2,
       verticals = TRUE
  )
  grid()
}))

all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  plot_usmap(data = allt, values = "impressions", color = "red") + 
    scale_fill_continuous(name = "Impressions", label = scales::comma) + 
    theme(legend.position = "right")
}))
#setDF(tmpp)

allt[, state := gsub(' ', '', state)]
allt[, len := nchar(state)]

tmpp = allt[, .(state,impressions, clickthroughs/impressions)]
setnames(tmpp, "V3", "CTrate")

ff = plot_usmap(regions = 'states', data =tmpp, values = 'impressions', labels = TRUE)
all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  print(ff)
}))


allt[, CTrate := clickthroughs/impressions]
all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  hist(as.numeric(allt$CTrate),
       main = 'Histogram of Clickthrough Rate by State',
       xlab= 'Clickthrough Rate',
       ylab = 'Frequency',
       col = 'steelblue'
  )
  grid()
}))

all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  plot(ecdf(allt$CTrate),
       main = 'ECDF of Clickthrough Rate State',
       xlab= 'Clickthrough Rate',
       ylab = 'Cumulative Proportion',
       col = 'steelblue',
       lwd = 3,
       cex = .2,
       verticals = TRUE
  )
  grid()
}))



ff = plot_usmap(regions = 'states', data =tmpp, values = 'CTrate', labels = TRUE)
all_res[[length(all_res) + 1]] <- list(img = capturePlot({
  print(ff)
}))

ff = plot_usmap(regions = 'states', data =tmpp, values = 'CTrate', labels = TRUE)

params <- list()
params$all_res <- all_res
params$set_title <- "Plot of Maps"
render(
  paste0(prefix, 'TEMPLATES/image_template_qual_var.Rmd'),
  output_file = paste0(prefix, 'geo_output.html'),
  output_dir = paste0(prefix, 'OUTPUT/'),
  quiet = FALSE,
  params = params
)
file.show(paste0(prefix, 'OUTPUT/geo_output.html'))
