library(sf)
library(data.table)
library(dplyr)
library(ggplot2)

################################## FUNCTIONS ##################################
act_on_layername <- function(lname,date){
  # purpose: reading and preprocessing layers
  # lname (char): string indicating layer name in gdb file
  # date (char): string indicating event date
  layer <- st_read("T:/Home/Shantanu/raw_data/wmp2020_psps.gdb",lname)
  layer <- st_transform(layer,crs=st_crs(nem_sf))
  l_dt <- as.data.table(layer)
  l_dt$event_date <- date
  if (length(colnames(layer))==13){
    l_dt[,FeederID:=NULL] # remove additional column found only in event 12
  } else if (lname==layer_names[13]){ # don't act on event 13 (no CircuitNam)
  } else { # replace FeederName NAs w/ CircuitNam (to reduce NAs for merge) 
    l_dt[is.na(FeederName),FeederName:=circuitNam]
  }
  # keep only the relevant columns
  return(as.data.frame(l_dt[,c("FeederName","Shape","event_date")]))
  # caution: NAs in FeederName for events 12 & 13 cannot be replaced 
  # CircuitNam column does not exist for these two events
}

pts_in_layer(feed_sf,nem_sf){
  # purpose: find feeder name (layer) for each installation (point)
  # feed_sf (sf object): layer 
  # nem_sf (sf object): point data
  # manually edit column names in function to match your datasets
  
  # PREP 
  # csv's to save post-event-analysis points
  suffix <- paste0("_",format(Sys.time(),"%m%y%d_%H%M"),".csv")
  allname <- paste0("NameByFeederALL",suffix)
  csvname <- paste0("NameByFeederONLY",suffix)
  # txt for progress statements/record
  txtname <- paste0(unlist(strsplit(allname,".csv")),"_log.txt")
  statsname <- paste0(unlist(strsplit(allname,".csv")),"_stats.csv")
  sink(txtname) # open progress log 
  
  # [POINTS IN LAYER
  for (idx in (1:length(feed_sf$FeederName)) ){
    # 'for' loop takes time, but prevents memory allocation error
    # the indexing is large (e.g. 1.4MB), and lapply didn't run successfully
    # progress statment
    if (!(idx%%50)){print(paste0("------------------------------",idx,":",Sys.time(),
                                 "------------------------------"))}
    start_time <- Sys.time()
    # attach circuit name to points in circuit 
    circuit <- feed_sf[idx,]$FeederName # works bc each Feeder Name is unique 
    print(paste0("-------------",circuit,"-------------"))
    idx <- st_within(nem_sf,subset(feed_sf,FeederName==circuit),sparse=FALSE)[,1]
    nem_dt[idx,c("circuit_name","treatment"):=.(circuit,1)] # NEW NEED TO CHECK
    # fwrite(nem_dt[idx],csvname,append=file.exists(csvname)) # write ALL points to csv
    # progress statement
    if (!(sum(idx))){ print("nothing to update")
    } else {print(paste0(sum(idx)," points updated with circuit name"))}
    # timing
    end_time <- Sys.time()
    diff_time <- format(end_time - start_time)
    print(paste("Feeder Run Time:",diff_time))
  }
  closeAllConnections() # close progress log (further output returns to console)
  
  # OUTPUT 
  # save ALL points
  fwrite(nem_dt,allname)
  # save only the points for which names were successfully attached
  fwrite(nem_dt[circuit_name!="circuit"],csvname,append=file.exists(csvname))
  # summary stats 
  nem_dt <- nem_dt[order(nem_dt$circuit_name)] # just to be sure
  n_tot <- nrow(nem_dt) # total number of points
  n_fail <- nrow(nem_dt[circuit_name=="circuit"]) # number of unnamed circuits 
  n_suc <- nrow(nem_dt[circuit_name!="circuit"]) # number of named points
  n_uniq <- length(unique(nem_dt[circuit_name!="circuit"]$circuit_name)) # number of unique feeders in points
  write.table(data.frame("total"=n_tot,"unnamed"=n_fail,
                         "named"=n_suc,"ufeeders"=n_uniq), statsname,
              col.names=!file.exists(statsname), row.names=FALSE,
              append=file.exists(statsname), sep=",", quote=FALSE,)
  closeAllConnections() # to be safe
}

################################# POINT DATA #################################
# Point Data: Interconnection Installations
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210402_1100")
nem_geo <- read.csv("CA CPUC NEM-PGE 020420 FulltoRel_18738na_osm.csv")
# start by providing circuit name column for all datapoints
nem_geo$circuit_name <- "circuit" # character type for data.table updating
nem_geo$treatment <- 0 # create dummy for control vs. treatment group
nem_notNA <- nem_geo[!is.na(nem_geo$cxy_lat),] # for conversion to sf 
nem_dt <- data.table(nem_notNA)
nem_sf <- st_as_sf(nem_notNA,coords=c("cxy_lon","cxy_lat"),crs=st_crs("NAD83"))
rm(nem_notNA)
rm(nem_geo)

################################# LAYER DATA #################################
# Polygon Data #1: all PG&E feeders (what I've been calling circuits/layer)
# st_layers("T:/Home/Shantanu/raw_data/ICADisplay.gdb")
feeders <- st_read("T:/Home/Shantanu/raw_data/ICADisplay_QGISbuffer_041621.shp")
feeders <- st_transform(feeders,crs=st_crs(nem_sf))
colnames(feeders)[colnames(feeders)=="Feeder_Nam"] <- "FeederName"
# prop for merging
feed_dt <- data.table(feeders)
feed_dt <- feed_dt[order(FeederName)] # ABC order by feeder name (convenience)
rm(feeders)

# Polygon Data #2: relevant PSPS layer in GDB folder 
# identify all (buffered) layers in .gdb
layer_names <- grep("Buffer",value=T,
                    st_layers("T:/Home/Shantanu/raw_data/wmp2020_psps.gdb")$name)
# read in all (buffered) layers
# prep for adding event date for each layer
event_dates <- c("10_09_19","10_10_19","10_23_19","10_23_19","10_23_19",
                 "10_24_19","10_26_19","10_27_19","10_29_19","10_29_19",
                 "11_20_19","10_05_19","06_07_19")

################################ PREPROCESSING ################################
# replace NAs in FeederName by CircuitNam when available
layer_list <- mapply(act_on_layername,layer_names,event_dates,SIMPLIFY=F)
# compile all layers (events) 
layer_comp <- rbindlist(layer_list)
layer_comp <- layer_comp[order(event_date,decreasing=T)] # most recent event 1st
layer_uniq <- unique(layer_comp,by="FeederName") # keep only most recent
layer_uniq_sf <- st_as_sf(layer_uniq)
layer_unique <- st_transform(layer_uniq_sf,crs=st_crs(nem_sf)) # change CRS
layer_unique_dt <- as.data.table(layer_unique) # data.table for merging
rm(layer_names,event_dates,act_on_layername,layer_list,layer_comp,layer_uniq,
   layer_uniq_sf,layer_unique)


# merge PSPS buffers onto all (buffered) feeders
feed_dt[FeederName=="ALLEGHANY 1101"]$geometry # check initial geometries
layer_unique_dt[FeederName=="ALLEGHANY 1101"]$Shape
feed_dt[layer_unique_dt,on=.(FeederName),"geometry":=i.Shape]
# check
feed_dt[FeederName=="ALLEGHANY 1101"]$geometry == layer_unique_dt[FeederName=="ALLEGHANY 1101"]$Shape
# sf object
feed_sf <- st_as_sf(feed_dt)
feed_sf <- st_transform(feed_sf,crs=st_crs(nem_sf))
length(feed_sf$FeederName) == length(unique(feed_sf$FeederName))
rm(feed_dt)

################################ POINT IN LAYER ################################
# FIND FEEDER NAME FOR EACH INSTALLATION BASED ON GIVEN EVENT
getwd()
st_crs(nem_sf) == st_crs(feed_sf,nem_sf)
pts_in_layer(feed_sf,nem_sf)
