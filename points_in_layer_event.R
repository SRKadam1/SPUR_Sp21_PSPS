# attach circuit name to (each) installation
library(sf)
library(data.table)
library(dplyr)
library(ggplot2)
library(units)

############################### Functions #####################################
# to store all feeder-named installations & stats for all PSPS events
totalname <- paste0("NamedOnly_",format(Sys.time(),"%m%y%d_%H%M"),".csv")
statsname <- paste0(unlist(strsplit(totalname,".csv")),"_stats.csv")

name_by_event <- function(totalname,statsname,
                          gdbname="T:/Home/Shantanu/raw_data/wmp2020_psps.gdb",
                          layername,nem_dt){
  # purpose: finds all points within geometries of event 
    # e.g. MULTIPOLYGON geometry of a feeder/circuit affected in a PSPS event
  # output: 
    # csv: after attaching feeder names, saves ALL points in nem_dt to  
      # txt: creates and updates a progress log in real-time
    # csv: after all feeders in event, saves ONLY points which got names attached
    # csv: after all feeders in event, saves summary stats for event
  # returns: points for which names were not attached 
  
  # totalname (char): name of csv to write all successfully-named points 
  # statsname (char): name of csv to write summary stats (for all events)
  # gdbname (char): geodatabase directory to pull events from
  # layername (char): PSPS event layer in gdb (e.g. feeders) !!!!!
  # nem_dt (data.table): points to attach names to (e.g. installations)
    # globally update and return at end
  
  # PREP  
  # csv to save post-event-analysis points
  csvname <- paste0("NameByFeeder",format(Sys.time(),"%m%y%d_%H%M"),".csv")
  # txt for progress statements/record
  txtname <- paste0(unlist(strsplit(csvname,".csv")),"_log.txt")
  sink(txtname) 
  # read in event layer
  event <- st_read(gdbname,layer=layername)
  event <- st_transform(event,crs=st_crs(nem_sf)) # for st_within
  event <- event[order(event$circuitNam),]
  # sort by feeder (convenient for real-time progress checks in txt)
  nem_dt <- as.data.table(nem_dt) # to be sure
  nem_dt <- nem_dt[order(nem_dt$circuit_name)] 
  
  # FIND FEEDER NAME FOR EACH INSTALLATION BASED ON GIVEN EVENT
  for (idx in (1:length(event$circuitNam)) ){
    # 'for' loop takes time, but prevents memory allocation error
      # the indexing is large (e.g. 1.4MB), and lapply won't run successfully
    # progress statment
    if (!(idx%%50)){print(paste0("------------------------------",idx,":",Sys.time(),
                                 "------------------------------"))}
    start_time <- Sys.time()
    # attach circuit name to points in circuit
    circuit <- event[idx,]$circuitNam
    print(paste0("-------------",circuit,"-------------"))
    idx <- st_within(nem_sf,subset(event,circuitNam==circuit),sparse=FALSE)[,1]
    nem_dt[idx,circuit_name:=circuit] # update global data.table 
    fwrite(nem_dt[idx],csvname,append=file.exists(csvname)) # write ALL points to csv
    # progress statement
    if (!(sum(idx))){ print("nothing to update")
    } else {print(paste0(sum(idx)," installations updated"))}
    # timing
    end_time <- Sys.time()
    diff_time <- format(end_time - start_time)
    print(paste("Feeder Run Time:",diff_time))
  }
  closeAllConnections() # close txt file (further output returns to console)

  # OUTPUT 
  # save only the points for which names were successfully attached
  fwrite(nem_dt[circuit_name!="circuit"],totalname,append=file.exists(totalname))
  # summary stats 
  nem_dt <- nem_dt[order(nem_dt$circuit_name)] # just to be sure
  n_tot <- nrow(nem_dt) # total number of points
  n_fail <- nrow(nem_dt[circuit_name=="circuit"]) # number of unnamed circuits 
  n_suc <- nrow(nem_dt[circuit_name!="circuit"]) # number of named points
  n_uniq <- length(unique(nem_dt[circuit_name!="circuit"]$circuit_name)) # number of unique feeders in points
  write.table(data.frame("total"=n_tot,"unnamed"=n_fail,
                         "named"=n_suc,"ufeeders"=n_uniq), statsname,
              col.names=!file.exists(statsname), row.names=layername,
              append=file.exists(statsname), sep=",", quote=FALSE,)
  # return all unnamed points (for piping into the next event)
  nem_dt <- nem_dt[circuit_name=="circuit"] # remove all feeder-named points
  closeAllConnections() # to be safe
  return(nem_dt) 
}

############################### Point Data #####################################
# Interconnection Installations
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210402_1100")
nem_geo <- read.csv("CA CPUC NEM-PGE 020420 FulltoRel_18738na_osm.csv")
# start by providing circuit name column for all datapoints
nem_geo$circuit_name <- "circuit" # character type for data.table updating
nem_notNA <- nem_geo[!is.na(nem_geo$cxy_lat),] # for conversion to sf 
nem_dt <- data.table(nem_notNA)
nem_sf <- st_as_sf(nem_notNA,coords=c("cxy_lon","cxy_lat"),crs=st_crs("NAD83"))
rm(nem_notNA)
rm(nem_geo)

############################## Polygon Data ####################################
# Polygon Data #1: all PG&E feeders (what I've been calling circuits)
all_layers <- grep("Buffer",value=T,
                   st_layers("T:/Home/Shantanu/raw_data/wmp2020_psps.gdb")$name)
?grep
event <- st_read("T:/Home/Shantanu/raw_data/wmp2020_psps.gdb",
                   layer="PSPS10_09_19D_02_Buffer_dis")
event <- st_transform(event,crs=st_crs(nem_sf)) # COME BACK TO
event <- event[order(event$circuitNam),]

############### merge relevant PSPS circuits onto all circuits##################
# add circuit name for all installations (that are in a circuit)
# attach circuit names from 1st outage event (is it the first?)
getwd()
csvname <- "name_circuits_41621_1110.csv"
sink("name_progress_log_41621_1110.txt")
# for the first one
circuit1 <- event[1,]$circuitNam
print(paste("-------------",circuit1,"-------------",sep=""))
idx1 <- st_within(nem_sf,subset(event,circuitNam==circuit1),sparse=FALSE)[,1]
nem_dt[idx1,circuit_name:=circuit1]
fwrite(nem_dt[idx1],csvname,append=FALSE) # update global data.table
if (!(sum(idx1))){ print("nothing to update")
} else {print("data table updated")}
rm(idx1)
rm(circuit1)
# for all after
for (idx in (2:length(event$circuitNam)) ){
  if (!(idx%%50)){print(paste0("------------------------------",idx,":",Sys.time(),
                               "------------------------------"))}
  start_time <- Sys.time()
  # attach circuit name to points in circuit
  circuit <- event[idx,]$circuitNam
  print(paste("-------------",circuit,"-------------",sep=""))
  idx <- st_within(nem_sf,subset(event,circuitNam==circuit),sparse=FALSE)[,1]
  nem_dt[idx,circuit_name:=circuit] # update global data.table
  fwrite(nem_dt[idx],csvname,append=TRUE) 
  if (!(sum(idx))){ print("nothing to update")
  } else {print(paste0(sum(idx),"installations updated"))}
  # timing
  end_time <- Sys.time()
  diff_time <- format(end_time - start_time)
  print(paste("Feeder Run Time:",diff_time))
}
closeAllConnections()
# did it work? (main goal: no error thrown by code)
nem_dt <- nem_dt[order(nem_dt$circuit_name)]
nrow(nem_dt[circuit_name!="circuit"]) # number of named circuits (goal: all)
length(unique(nem_dt[circuit_name!="circuit"]$circuit_name))
nrow(nem_dt) # how many circuits were originally unnamed
nrow(nem_dt[circuit_name=="circuit"]) # number of unnamed circuits (goal: 0)
# save/remove all installations that are now circuit-named
# run code again with new PSPS event
# repeat until all addresses circuit-named
# save output 
outpfx <- paste0("NamedByCircuit_",format(Sys.time(),"%d%m%y_%H%M"),"_100919buffer")
fwrite(nem_dt,paste0(outpfx,".csv")) # geometry might not save properly... 
nem_sf <- nem_sf[nem_dt$circuit_name=="circuit",]
nem_dt <- nem_dt[circuit_name=="circuit"] # remove all circuit-named points 
temp_csv <- read.csv("NamedByCircuit_170421_0119_100919buffer.csv")
temp_dt <- as.data.table(temp_csv)
fwrite(temp_dt[circuit_name!="circuit"],"NamedOnly_41621.csv") # only circuit-named
rm(temp_csv,temp_dt) 

### 
st_layers("T:/Home/Shantanu/raw_data/wmp2020_psps.gdb")
event <- st_read("T:/Home/Shantanu/raw_data/wmp2020_psps.gdb",
                 layer="PSPS10_26_19D_04_Buffer_dis")
event <- st_transform(event,crs=st_crs(nem_sf)) # COME BACK TO
event <- event[order(event$circuitNam),]
csvname <- "name_circuits_41721_0152.csv"
sink("name_progress_log_41721_0152.txt")
# for the first one
circuit1 <- event[1,]$circuitNam
print(paste("-------------",circuit1,"-------------",sep=""))
idx1 <- st_within(nem_sf,subset(event,circuitNam==circuit1),sparse=FALSE)[,1]
nem_dt[idx1,circuit_name:=circuit1]
fwrite(nem_dt[idx1],csvname,append=FALSE) # update global data.table
if (!(sum(idx1))){ print("nothing to update")
} else {print("data table updated")}
rm(idx1)
rm(circuit1)
# for all after
for (idx in (2:length(event$circuitNam)) ){
  if (!(idx%%50)){print(paste0("------------------------------",idx,":",Sys.time(),
                               "------------------------------"))}
  start_time <- Sys.time()
  # attach circuit name to points in circuit
  circuit <- event[idx,]$circuitNam
  print(paste("-------------",circuit,"-------------",sep=""))
  idx <- st_within(nem_sf,subset(event,circuitNam==circuit),sparse=FALSE)[,1]
  nem_dt[idx,circuit_name:=circuit] # update global data.table
  fwrite(nem_dt[idx],csvname,append=TRUE) 
  if (!(sum(idx))){ print("nothing to update")
  } else {print(paste0(sum(idx)," installations updated"))}
  # timing
  end_time <- Sys.time()
  diff_time <- format(end_time - start_time)
  print(paste("Feeder Run Time:",diff_time))
}
closeAllConnections()
# did it work? (main goal: no error thrown by code)
nem_dt <- nem_dt[order(nem_dt$circuit_name)]
nrow(nem_dt[circuit_name!="circuit"]) # number of named circuits (goal: all)
length(unique(nem_dt[circuit_name!="circuit"]$circuit_name))
nrow(nem_dt) # how many circuits were originally unnamed
nrow(nem_dt[circuit_name=="circuit"]) # number of unnamed circuits (goal: 0)
# save/remove all installations that are now circuit-named
# run code again with new PSPS event
# repeat until all addresses circuit-named
# save output 
fwrite(nem_dt[circuit_name!="circuit"],"NamedOnly_41621.csv",append=TRUE)
outpfx <- paste0("NamedByCircuit_",format(Sys.time(),"%d%m%y_%H%M"),"_100919buffer")
fwrite(nem_dt,paste0(outpfx,".csv")) # geometry might not save properly... 
st_write(nem_dt,paste0(outpfx,".shp")) # idk but should save properly 
nem_sf <- nem_sf[nem_dt$circuit_name=="circuit",]
nem_dt <- nem_dt[circuit_name=="circuit"] # remove all circuit-named points 

################### plot nem_dt by circuit for all circuits ####################
# test w/ smaller group
# test_dt <- nem_dt[circuit_name!="circuit"]
test_dt <- fread("NamedOnly_41621.csv")
test_dt$Interconnection.Date <- as.POSIXct(test_dt$Interconnection.Date,format="%m/%d/%Y")
test_dt <- test_dt[order(circuit_name)]
# prep vertical lines
event_dates <- c("10_09_2019","10_10_2019","10_23_2019","10_24_2019",
                 "10_26_2019","10_27_2019","10_29_2019","11_20_2019",
                 "06_07_2019","10_05_2019")
event_dates <- as.POSIXct(event_dates,format="%m_%d_%Y")
startdate <- as.POSIXct("01_01_2017",format="%m_%d_%Y")
enddate <- as.POSIXct("01_01_2020",format="%m_%d_%Y")
# take t_dt, reorder rows using date, calculate cumulative capacity, grouped by circuit 
tt_dt <- test_dt %>% 
  group_by(circuit_name) %>%
  arrange(Interconnection.Date,.by_group=TRUE) %>% 
  mutate(cum_AC_capacity=cumsum(AC.Capacity..kW.))
tt_dt[c("circuit_name","Interconnection.Date","AC.Capacity..kW.","cum_AC_capacity")]
tt_dt$Interconnection.Date <-as.POSIXct(tt_dt$Interconnection.Date,format="%m/%d/%Y")
tt_dt$circuit_name <- factor(tt_dt$circuit_name)
# add total 2019 outage hours for each circuit
circhrs <- read.csv("T:/Home/Shantanu/raw_data/TotalOutageHrsByCircuit.csv")
colnames(circhrs) <- c("X","circuit_name","outage_hours")
tt_dt <- as.data.table(tt_dt)
tt_dt[circhrs,on=.(circuit_name),"outage_hrs":=outage_hours]
tt_dt$hrs50 <- 0
tt_dt[outage_hrs>50,"hrs50":=1]
# stats
nrow(tt_dt[outage_hrs>50])
nrow(tt_dt[hrs50==1])
nrow(tt_dt)
length(unique(tt_dt$circuit_name)) 
# plot
ggplot(tt_dt,aes(x=Interconnection.Date,y=cum_AC_capacity,group=circuit_name,
                 col=factor(hrs50))) + 
  geom_line() + 
  xlim(startdate,enddate) +
  geom_vline(xintercept=event_dates,col='black') + # linetype='dotted'
  labs(x='Date',y='Capacity (KWh)',title='Home Installations') 
# check outliers
outlierdate <- as.POSIXct("01_01_2010",format="%m_%d_%Y")
unique(tt_dt[Interconnection.Date<outlierdate][cum_AC_capacity>30000]$circuit_name)
tt_dt[circuit_name=="MADISON 2101"]$cum_AC_capacity

# check installation type: not just solar! 
unique(nem_dt$Generator.Technology.Type)
