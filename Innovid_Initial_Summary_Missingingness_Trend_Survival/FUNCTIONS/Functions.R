get_data <- function(con, query) {
  con <- dbConnect(RPostgres::Postgres(), dbname = con$db, host=con$host_db, port=con$db_port, user=con$db_user, password=con$db_password)
  
  res = dbGetQuery(con, query)
  setDT(res)
  return(res)
}

get_con <- function(db_user = NULL, db_password = NULL) {
  db <- 'ngca'  #provide the name of your db
  host_db <- 'trulicity-rs-cluster.cq3ibvgeajww.us-east-2.redshift.amazonaws.com'
  db_port <- '5439'  # or any other port specified by the DBA
  
  return(list(db = db, host_db = host_db, db_port = db_port, db_user = db_user, db_password = db_password))
  
}



commasep <- function(dat) {
  cat("----------------------------------------\n")
  cat("Copy and paste this into Excel\n")
  cat("-----------------------------------------\n")
  
  x = copy(dat)
  if(is.data.frame(x)) {
    data.table::setDF(x)
  }
  
  x[] <- lapply(x, function(x) { if(is.numeric(x)){ x = as.character(x);}; return(x)})
  
  rn <- row.names(x)
  cat(glue::glue_collapse(colnames(x), sep = "|"), "\n")
  for(i in 1:nrow(x)) {
    cat(rn[i], "|", glue::glue_collapse(x[i,], sep ="|"), "\n")
  }
  cat("\n")
}



inhouse_styling <- function(dat) {
  kable(dat) %>% kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive"))
}

convert_table <- function(x) {
  #x        <- table(x)
  #fx       <- names(x)
  names(x) <- "Variable"
  #x[1]     <- fx
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



find_variables_to_collapse <- function(cname, thresh=8) {
  # Find all unique names
  
  # Create an adjacency matrix showing string distances.
  mat = matrix(100, length(cname), length(cname))
  for(i in 1:length(cname)) {
    mat[i, ] <- (adist(cname[i], cname))
  }
  
  # Construct a rule to find those string distances that are close
  # to each other.
  test = function(x) { cname[which(x < thresh)] }
  close_distances = apply(mat, 1, test)
  
  close_distances[[length(close_distances)>0]]
  length(close_distances$`Interceptor_Plus-Preroll-WebRes`)
  
  # remove those that had no matches.
  idx = which(sapply(close_distances, function(x) { length(x) > 1}) == TRUE)
  close_distances = close_distances[idx]
  
  # Now that we have that, lets create unique sets of this.
  # Note: this does not guarantee that some elements in one set won't be shared
  # with elements in the other set. 
  close_distances = unique(close_distances)
  
  # Now check to see if there are any intersections that we can use to
  # collapse them.
  do_recusrive <- function(close_distances) {
    newlist = list()
    skipthese <- c(NA)
    for(i in 1:length(close_distances)) {
      #i = 7
      if(i %in% skipthese) { next;}
      fbad = close_distances[[i]]
      
      ids = sapply(close_distances, function(x) {length(intersect(x, fbad))})
      
      if(length(ids>0)) {
        doit <- sort(unique(unlist(close_distances[which(ids>0)])))
        
        skipthese <- c(skipthese, which(ids>0))
        skipthese <- setdiff(skipthese, i)
        
        newlist[[length(newlist)+1]] <- doit
      } else {
        newlist[[length(newlist)+1]] <- fbad
      }
    }
    newlist <- unique(newlist)
    return(newlist)
  }
  for(i in 1:20) {
    close_distances <- do_recusrive(close_distances);
  }
  
  return(close_distances)
  
}

plot_time_with_bp <- function(
  t1,
  ylab = NULL, 
  main = NULL, 
  xlab = 'Date',
  ylim = NULL, 
  add = FALSE, 
  col = 'steelblue', 
  change_points = TRUE) {
  
  alldates <- data.table(date = seq.Date(min(t1$date), max(t1$date), by= 1))
  t1       <- merge(alldates, t1, by = 'date', all.x = TRUE)
  t1[is.na(V1), V1 := 0]
  
  setorder(t1, date)
  
  # Compute change points
  result = tryCatch({
    fit_bp = strucchange::breakpoints(t1$V1 ~ 1);
  }, warning = function(w) {
    fit_bp = list()
    fit_bp$breakpoints = 0
    fit_bp
  }, error = function(e) {
    fit_bp = list()
    fit_bp$breakpoints = 0
    fit_bp
  }, finally = {
    fit_bp = list()
    fit_bp$breakpoints = 0
    fit_bp
  }
  )
  
  if(add == FALSE) {
  
    plot(t1, 
         type = 'l',
         ylab = ylab,
         main = main,
         xlab = xlab,
         lwd  = 3,
         col  = col,
         ylim = ylim 
    )
  } else {
    lines(t1,
          type = 'l',
          lwd  = 3,
          col  = col,
          ylim = ylim 
    )
  }
  grid()
  if(length(result$breakpoints)>0 & change_points == TRUE) {
    abline(v=min(t1$date) + result$breakpoints, col = 'red')
  }
}

plot_time_with_bp_capture <- function(
  t1,
  ylab = NULL, 
  main = NULL, 
  xlab = 'Date',
  ylim = NULL, 
  add = FALSE, 
  col = 'steelblue', 
  change_points = TRUE, 
  dt_range = NULL) {
  
  if(is.null(dt_range)) {
    alldates <- data.table(date = seq.Date(min(t1$date), max(t1$date), by= 1))
  } else {
    alldates <- data.table(date = seq.Date(dt_range[1], dt_range[2], by= 1))
  }
  t1       <- merge(alldates, t1, by = 'date', all.x = TRUE)
  t1[is.na(V1), V1 := 0]
  
  setorder(t1, date)
  
  # Compute change points
  result = tryCatch({
    fit_bp = strucchange::breakpoints(t1$V1 ~ 1);
  }, warning = function(w) {
    fit_bp = list()
    fit_bp$breakpoints = 0
    fit_bp
  }, error = function(e) {
    fit_bp = list()
    fit_bp$breakpoints = 0
    fit_bp
  }, finally = {
    fit_bp = list()
    fit_bp$breakpoints = 0
    fit_bp
  }
  )
  res = R.devices::capturePlot({  
  #if(add == FALSE) {

    plot(t1, 
         type = 'l',
         ylab = ylab,
         main = main,
         xlab = xlab,
         lwd  = 3,
         col  = col,
         ylim = ylim 
    )
  # } else {
  #   lines(t1,
  #         type = 'l',
  #         lwd  = 3,
  #         col  = col,
  #         ylim = ylim 
  #   )
  # }
  grid()
  if(length(result$breakpoints)>0 & change_points == TRUE) {
    abline(v=min(t1$date) + result$breakpoints, col = 'red')
  }
  })
  
return(res)
}


athena_background  <- function(origquery, prefix) {
  library(data.table)
  library(RAthena)
  library(reticulate)
  library(aws.signature)
  reticulate::use_condaenv()  
  
  
  source(paste0(prefix, 'standard_config.R'))
  source(paste0(prefix, 'FUNCTIONS/Functions.R'))
  
  use_credentials(file=AWS_keys_location)  
  con <- dbConnect(RAthena::athena(),
                   s3_staging_dir=s3_staging_dir,
                   region_name='us-east-2')
  
  (dat4     <- dbGetQuery(con, origquery))
  #fwrite(dat4, paste0(prefix, 'OUTPUT/all_data_date.csv'))
  return(dat4)
}


prep_table <- function(depvar = NULL, date_var = NULL, condit_var = NULL) {
  
  if(is.null(condit_var)) {
    t1 = dat0[, .(sum(get(depvar))), get(date_var)]
  } else {
    ivars = c(date_var, condit_var)
    t1 = dat0[, .(sum(get(depvar))), mget(date_var)]
  }
  setnames(t1, "get", date_var)
  return(t1)
}

