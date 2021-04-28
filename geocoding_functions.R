###############################################################################
################################# FUNCTIONS ###################################
###############################################################################
library(dplyr)
library(postmastr)
library(censusxy)
library(tidycensus)
library(tmaptools)
library(data.table)
library(remotes)
# install_github("slu-openGIS/censusxy") # need censusxy >= v1.0.2 
library(parallel)
library(foreach)
library(doParallel)
detectCores() # Skynet has 48 cores 
numCores <- 4 # seems like an ok amount to be using? (Skynet load generally low
# expectation: cxy_geocode will automatically use numCores

# quality of life improvements (to increase readability)
get_data <- function(filename){
  # purpose: get data from filename that is useful for geocoding
  data <- read.csv(filename,stringsAsFactors=F) # read in data
  data$Plant.State <- "CA" # add in state (for Census API)
  # use only relevant data to make geocoding faster
  rel_data <- subset(data,select = c(Plant.Address,Plant.City,Plant.State,Plant.Zip))
  return(rel_data)
}

# improve geocoding process (write functions)
base_geocoding <- function(split_df,int_idx,max_idx,columns,verbose=F){
  # purpose: geocodes a dataframe using censusxy::geocode
  # split_df (df): a dataframe to geocode
  # geocoding() passes in a list of dataframes ("grouped_df")
  # int_idx (int): index of current group split_df
  # max_idx (int): number of batches (in current group) to geocode
  # columns (list of characters): column names in full_df
  # required order: street, city, state, zip
  # verbose (logical): if true, prints progress statements 
  # prints: batch run time, group run time, # NAs
  
  # PREP (progress message)
  progress_idx <- paste(int_idx,max_idx,sep="/")
  progress_msg <- paste("geocoding",progress_idx,"at",
                        format(Sys.time(),"%H:%M:%S"),sep=" ")
  print(progress_msg)
  
  # GEOCODE
  start_time <- Sys.time()
  # geo <- cxy_geocode(split_df,street="std_addr",city="Plant.City",
  #                    state="pm.state",zip="pm.zip",output="simple")
  geo <- cxy_geocode(split_df,street=columns[[1]],city=columns[[2]],
                     state=columns[[3]],zip=columns[[4]],output="simple",
                     parallel=numCores)
  end_time <- Sys.time()
  
  # OUTPUT
  if (verbose){
    time_msg <- paste("batch run time:",
                      round(difftime(end_time,start_time,units="mins"),3),"mins")
    print(time_msg)
  }
  return(geo)
}

geocoding <- function(list_dfs,gp_idx,max_gp_idx,columns,std_bool=T,
                      verbose=F,na_bool=F,return_bool=F){
  # list_dfs (list of df's): list of dataframes to geocode 
  # each dataframe is individually sent to base_geocoding()
  # gp_idx (int): index of current grouping list_dfs
  # max_gp_idx (int): number of groups to geocode in total
  # columns (list of characters): column names in full_df
  # required order: street, city, state, zip
  # std_bool (logical): set TRUE when list_dfs has standardized (street) addresses
  # verbose (logical): if true, prints progress statements 
  # prints: batch run time, group run time, # NAs
  # na_bool (logical): if true, returns na_geo
  # ne_geo (df): rows for compiled_geo that failed to geocode
  # return_bool (logical): if true, returns compiled_geo
  # compiled_geo (df): geocoded data for all batches in group
  
  # PREP
  # progress of grouped dataframes
  gp_progress_idx <- paste(gp_idx,max_gp_idx,sep="/")
  gp_progress_msg <- paste("------ group",gp_progress_idx,"at",
                           format(Sys.time(),"%H:%M:%S"),"------",sep=" ")
  print(gp_progress_msg)
  # setup progress of split dataframes
  int_idx <- as.integer(names(list_dfs))+1
  max_idx <- max(int_idx)
  
  # GEOCODE
  # print("starting geocoding...")
  start_time2 <- Sys.time()
  geo <- mapply(base_geocoding,list_dfs,int_idx,max_idx,
                MoreArgs=list(columns,verbose),
                USE.NAMES=T,SIMPLIFY=FALSE) # return vectors, not lists 
  end_time2 <- Sys.time()
  # print("finished geocoding...")
  if (verbose){
    time_msg2 <- paste("group run time:",
                       round(difftime(end_time2,start_time2,units="mins"),3),
                       "min")
    print(time_msg2)
  }
  
  # OUTPUT
  compiled_geo <- do.call(rbind,geo) # compile all batches in group
  # summary of geocoder success
  na_geo <- compiled_geo[is.na(compiled_geo$cxy_lon),]
  n_na <- nrow(na_geo) # number of NAs
  if (verbose) print(paste("# of addresses not geocoded:",n_na))
  # save geocoded composite/compiled dataframe
  if (std_bool) {prefactor <- "std" # used standardized (street) addresses?
  } else {prefactor <- "unstd"}
  output <- paste(prefactor,gp_idx,"_",outname,"_",n_na,"na",".csv",sep="")
  # save data
  write.csv(compiled_geo,output)
  print(paste("grouping written to file:",output,
              "at",format(Sys.time(),"%H:%M:%S")))
  if (na_bool && return_bool) { # return geocoded & NAs
    return(list(compiled_geo,na_geo))
  } else if (na_bool) { # return just NA rows
    return(na_geo) 
  } else if (return_bool) { # return just geocoded
    return(compiled_geo) # returns as list (0-indexing) 
  }
  
}

wrap_geocoding <- function(dirname,full_df,len=1000,grouping=100,columns,
                           std_bool=T, verbose=F, na_bool=F, return_bool=F){
  # purpose: progress statements, saving data, and general wrapping geocoding
  # dirname (character): name of folder containing full_df's source file
  # geocoding() changes working directory to save geocoded data
  # if running wrap_geocoding in succession, need to reset directory
  # full_df (df): dataframe to geocode
  # should contain the following columns for cxy_geocode():
  # street="std_addr",city="Plant.City",state="pm.state",zip="pm.zip"
  # len (int): length (# rows) for each batch sent to Census Geocoder
  # use to split full_df into smaller/"split" dataframes
  # cannot exceed 10,000
  # grouping (int): conceptually, how often to save geocoded data
  # how many smaller/"split" dataframes to put in each group 
  # columns (list of characters): column names in full_df
  # required order: street, city, state, zip
  # std_bool (logical): see geocoding()
  # verbose (logical): see geocoding()
  # na_bool (logical): input for geocoding()
  # if true, returns rows that failed to geocode (useful for inspection)
  # return_bool (logical): input for geocoding()
  # if true, returns dataframes (useful for inspecting data)
  
  # PREP
  setwd(dirname) # reset current directory
  start_time3 <- Sys.time()
  # batch geocoding allows max length of 10,000
  print(paste("# total rows:",nrow(full_df)))
  split_msg <- paste("splitting by batch length:",len,sep=" ")
  print(split_msg)
  split_dfs <- split.data.frame(full_df,(0:nrow(full_df)) %/% len)
  print(paste("# split dataframes:",length(split_dfs)))
  # output folder
  datefolder <- paste(format(Sys.Date(),"%y%m%d"),format(Sys.time(),"%H%M"),sep="_")
  outfolder <- paste(outname,datefolder,sep="_")
  dir.create(outfolder)
  setwd(paste(getwd(),outfolder,sep="/"))
  folder_msg <- paste("output folder",outfolder)
  print(folder_msg)
  # group split dfs for compiling and saving
  # 0:9 has 10 elements, so 0:length(0:9) = 0,1,2,...,10 has 11 elements!
  # split() will add one fake entry => 0:length()-1 below
  # (not an issue for split.data.frame)
  grouped_dfs <- split(split_dfs,(0:(length(split_dfs)-1))%/%grouping)
  print(paste(length(grouped_dfs),"groups of at most",
              grouping,"batches ('split dfs')"))
  # setup progress of grouped dataframes
  gp_idx <- as.integer(names(grouped_dfs))+1
  max_gp_idx <- max(gp_idx)
  
  # GEOCODE
  list_gp_geo <- mapply(geocoding,
                        list_dfs=grouped_dfs,
                        gp_idx=gp_idx,max_gp_idx=max_gp_idx,
                        MoreArgs=(list(columns,std_bool,verbose)),
                        return_bool=return_bool,SIMPLIFY=F)
  end_time3 <- Sys.time() 
  time_msg3 <- paste("total run time:",
                     round(difftime(end_time3,start_time3,units="hours"),3),
                     "hours")
  
  # OUTPUT
  # summary (bc easier to see at end)
  print("-----------summary-------------")
  print(paste("# total rows:",nrow(full_df)))
  print(split_msg)
  print(paste(length(grouped_dfs),"groups of at most",
              grouping,"batches ('split dfs')"))
  print(folder_msg)
  print(time_msg3)
  # data
  if (is.null(list_gp_geo)) { # if return_bool is false
    print("no data returned")
  } else { # if return_bool is true
    return(list_gp_geo)
  }
}

sleepy_OSM <- function(addr){
  # purpose: Census API doesn't geocode everything, so use this as 2nd geocoder
  # caution: Nominatim has strict usage policy 
  # https://operations.osmfoundation.org/policies/nominatim/
  Sys.sleep(1)
  # if (idx%%100) print(paste("addresses attempted:",idx))
  # return(suppressMessages(geocode_OSM(q=addr,keep.unfound=T,as.data.frame=T)[,c("query","lat","lon")]))
  return(suppressMessages(geocode_OSM(q=addr,projection=st_crs("NAD83"),keep.unfound=T,as.data.frame=T)[,c("query","x","y")]))
}

OSM_geocoding <- function(filename,return_bool=F){
  # purpose: progress statements, OSM geocoding
  # merges back onto Census geocoded addresses
  
  # PREP
  d1 <- read.csv(filename) # list
  d1 <- as.data.frame(d1) # dataframe
  # create full_addr column in postmastr data (call "query" to match OSM output)
  d1$query <- paste(d1$std_addr,d1$pm.city,d1$pm.state,d1$pm.zip,sep=", ")
  # isolate NAs
  d1_na <- d1[is.na(d1$cxy_lon),] 
  setDT(d1) # prep for update join
  # unique addresses
  na_unique <- d1_na %>% distinct(query,.keep_all=T)
  # MIGHT HELP IN PREPPING CENSUS GEOCODER TOO!!!
  na_unique$query <- gsub("#","",na_unique$query) # throws error for OSM parsing
  
  
  # GEOCODE
  print(paste("OSM Geocoding", nrow(na_unique), "addresses"))
  print(paste("Estimated Duration:", nrow(na_unique)/3660, "hr"))
  print(paste("Current Time:", Sys.time()))
  osm_out <- do.call(rbind,lapply(na_unique$query,sleepy_OSM))
  # success stat
  unique_success_msg <- paste("# NA's in osm_out:",sum(is.na(osm_out$lon))) 
  print(unique_success_msg)
  
  # OUTPUT
  # merge geocoded results back into postmastr-ed data
  setDT(osm_out) # prep for update join
  # d1[osm_out,on=.(query),c("cxy_lon","cxy_lat"):=.(i.lon,i.lat)] # update join
  d1[osm_out,on=.(query),c("x","y"):=.(i.lon,i.lat)] # update join
  # success stat
  osm_na <- sum(is.na(d1$cxy_lon))
  total_success_msg <- paste("# NA's in d1(post-OSM):", osm_na)
  print(total_success_msg)
  # save file
  osm_prefix <- unlist(strsplit(filename,".csv"))[1]
  osm_filename <- paste(osm_prefix,"to",osm_na,"na_osm.csv",sep="")
  write.csv(d1,osm_filename)
  print(paste("written to",osm_filename))
  # return
  if (return_bool) return(osm_out)
}

