# load geocoding functions (and libraries)
source("T:/Home/Shantanu/scripts/geocoding_functions.R")

# general inputs
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM")
dirname = getwd() 
filename = "CA CPUC NEM-PGE 020420.csv" # input file name
colname = "pm.address.nem" # column name for standardized address in new dataset 
outname = "CA CPUC NEM-PGE 020420" # output file name 

# basic geocoding & input to postmastr
rel_data <- read.csv(filename,stringsAsFactors=F) # read in data
rel_data$State <- "CA" # add in state (for Census API)
# use only relevant data to make geocoding faster
rel_data <- subset(rel_data,select = c(Service.Address,City,State,Zip))

############################### basic geocoding ###############################
unstd <- list(geo=NA,NAs=NA) 
unstd[c("geo","NAs")] <- wrap_geocoding(dirname,rel_data,len=8000,grouping=5,
                                  columns=c("Service.Address","City", "State","Zip"),
                                  std_bool=F,verbose=T,na_bool=F,return_bool=F)

############################# standardize address #############################
# full/"long" address (don't know how to standardize city outside piping)
rel_data$full_addr <- paste(rel_data$Service.Address,rel_data$City,
                            rel_data$State,rel_data$Zip,sep=", ")
# create dictionaries for parsing
census_api_key("8fd77e8a4273f7d241b3d38fc54839250846682a",install="False") # needed?
cdict <- pm_dictionary(type = "city",filter="CA",
                       case=c("title","lower","upper"),locale = "us")
CAdict <- pm_dictionary(type="state", filter = "CA",
                        case = "title", locale = "us")
# attach unique IDs to data (enables parsing)
data_id <- rel_data %>% 
  pm_identify(var=full_addr)
# parse & standardize data
data_std <- data_id %>% # will throw mutate() errors
  pm_parse(input="full", address=full_addr, output="short", 
           new_address=std_addr, keep_parsed="limited", side="left",
           # houseSuf_dict = , street_dict = # define for each dataset!!
           # dir_dict = , suffix_dict = , # uses full dict if unspecified
           city_dict=cdict, state_dict=CAdict)
# when trying to figure out final addr form: use this I think
# data_std <- data_id %>% # will throw mutate() errors
#   pm_parse(input="full", address=full_addr, output="short", 
#            new_address=std_addr, keep_parsed="limited", side="left",
#            # houseSuf_dict = , street_dict = # define for each dataset!!
#            # dir_dict = , suffix_dict = , # uses full dict if unspecified
#            city_dict=cdict, state_dict=CAdict)
std_filename <- paste("std_",outname,"_",Sys.time(),".csv",sep="")
write.csv(data_std,std_filename)
# standardizing a "level" requires dictionary for that "level"
# if none provided and no default, then that "level" will NOT be parsed/standardized!
# must check for your data: examine data not included in dictionary (see below)

####################### standardized address geocoding ########################
# use only relevant data to make batch geocoding faster
# !!!!!!manually change the input for select below!!!!!! 
std <- list(geo=NA,NAs=NA) 
std[c("geo","NAs")] <- wrap_geocoding(dirname,data_std,len=8000,grouping=4,
                            columns=c("std_addr","pm.city","pm.state","pm.zip"),
                            std_bool=T,verbose=T,na_bool=F,return_bool=F)

####################### comparison of geocoding ########################
# Summary: NAs (as listed in titles of files)
total <- nrow(rel_data)
# get summary statistics from saved file names
uvec <- c(4237,3840,3027,2772,2571,3026,
         2962,3061,3967,3523,3894,4431,1387)
svec <- c(3592,3257,3622,2426,2441,5183,2511,2567,2377,
          2661,2909,3277,2909,3277,2959,3205,3602,1358)
uNA <- sum(uvec)
sNA <- sum(svec)
oNA <- 18738
ufail <- uNA/total
sfail <- sNA/total
ofail <- oNA/total
sum_NA <- data.frame(NAs=c(uNA,sNA,oNA),percent=c(ufail,sfail,ofail),
           success=1-c(ufail,sfail,ofail),
           row.names=c("unstd","std","osm"))
sum_NA

# Conclusions
# only 10% of data not geocoded!!!
# improving standardization could help?
# why is unstandardized geocoding more successful than standardized???
## possible solution: try running std again w/ parallelization

# Inspections
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210331_2318")
d1 <- read.csv("std10_CA CPUC NEM-PGE 020420_7392na.csv") # list
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210401_1809")
d2 <- read.csv("unstd10_CA CPUC NEM-PGE 020420_3523na.csv")
d1 <- as.data.frame(d1) # dataframe
d2 <- as.data.frame(d2) # dataframe
d1_na <- d1[is.na(d1$cxy_lon),] 
d2_na <- d2[is.na(d2$cxy_lon),] 
nrow(d1_na) #7392
nrow(d2_na) #3523
length(unique(d1_na$std_addr)) #5788
length(unique(d2_na$Service.Address)) #3185
head(d1_na$std_addr)
head(d2_na$Service.Address)
d1_na[1,c("std_addr","cxy_lon","cxy_lat")]
d2[,]
sum(d2$Service.Address=="1707 East St")
which(d1$std_addr=="1707 East St")
d1[1,]
d2[1,]
# Conclusion: upon manual entry, both give proper geocoding
# but only unstd gave it in code/script
# solution(?): run std geocoding again + parallelization

# Conclusion: Census Geocoder doesn't have certain addresses
# Solution: use another geocoder (OpenStreetMaps)
# !!note: OpenStreetMaps return lat,lon (vs lon,lat order of Census)!!
library(tmaptools)
# What are the unique addresses?
na_unique <- d1_na %>% distinct(std_addr,Plant.City,pm.state,pm.zip,.keep_all=T)
na_unique$full_addr <- paste(na_unique$std_addr,na_unique$Plant.City,
                             na_unique$pm.state,na_unique$pm.zip,sep=", ")
# MIGHT HELP IN PREPPING CENSUS GEOCODER TOO!!!
na_unique$full_addr <- gsub("#","",na_unique$full_addr) # throws error for OSM parsing
na_unique$idx <- as.integer(rownames(na_unique))
head(na_unique)

# Can we geocode them?
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210402_1100")
OSM_geocoding("CA CPUC NEM-PGE 020420 CensusRel.csv")

############################## post-processing ###############################
# combine all groups of batches back into one big dataframe
# https://datascienceplus.com/how-to-import-multiple-csv-files-simultaneously-in-r-and-create-a-data-frame/
all_csv <- list.files(path="T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210402_1100",
           pattern="*.csv",full.names=TRUE)
comp_files <- ldply(all_csv,read.csv) # returns 1 proper dataframe?? 
# alternative (https://stackoverflow.com/questions/11433432/how-to-import-multiple-csv-files-at-once)
myfiles <- lapply(all_csv, read.csv) # returns 1 dataframes per file
myfile <- do.call(rbind,myfiles)

# remove irrelevant data (created for geocoding above)
# should really delete before saving data to files in functions or above... 
names(myfile)
d <- subset(d,select=-c("X","State","full_addr")) # for ustd

rel_cols <- c("std_addr","Interconnection.Application.ID", "Service.Address",
              "City","County","Zip","Customer.Rate.Class","Project.Program.Type",
              "Aggregation","Generator.Technology.Type","Equipment.Program.Type",
              "Interconnection.Date","DC.Capacity..kW.","AC.Capacity..kW.",
              "Incentive.Program..if.applicable.","Equipment.Deactivation.Date",
              "Generator.Still.at.Site.","Other.","cxy_lon","cxy_lat")
# for std 
myfile_rel <- subset(myfile,select= rel_cols)
write.csv(myfile,paste(outname,"CensusFull.csv"))
write.csv(myfile_rel,paste(outname,"CensusRel.csv"))
myfile_rel[is.na(myfile_rel$cxy_lon),]

################################ plotting in R #################################
######################## (spatial analysis of geocode) #########################
setwd("T:/Home/Shantanu/intermediate_data/Project2_NEM/CA CPUC NEM-PGE 020420_210402_1100")
geo_data <- read.csv("CA CPUC NEM-PGE 020420 CensusFullto18738na_osm.csv")
names(geo_data) # any irrelevant columns? lots bc used CensusFull for OSM...
library(sf)
st_crs("NAD83")
data_notNA <- geo_data[!is.na(geo_data$cxy_lat),]
data_sf <- st_as_sf(data_notNA,coords=c("cxy_lon","cxy_lat"),crs=st_crs("NAD83"))
plot(data_sf$geometry)
ggplot() + 
  geom_sf() +
  geom_point(data=outCA,aes(cxy_lon,cxy_lat),
             size=1,fill="darkred") + 
  coord_sf(xlim=c(-120,150),ylim=c(-40,65))
names(geo_data)
sel_geo_data <- subset(geo_data,select=rel_cols)
write.csv(sel_geo_data,"CA CPUC NEM-PGE 020420 FulltoRel_18738na_osm.csv")

library("ggplot2")
theme_set(theme_bw())
# library("rnaturalearth")
# library("rnaturalearthdata")
# devtools::install_github("ropensci/rnaturalearthhires") 
# 
# states <- ne_states(country="united states of america")
# names(CAgeom)
# CAgeom <- st_as_sf(states[states$name == "California",])
# CAgeom$area_sqkm
# names(states)
# class(CAgeom)
library("spData")
CAgeom <- us_states[us_states$NAME=="California",]
st_contains(data_sf,CAgeom)
st_contains(data_sf,CAgeom)
outCA <- data_sf[!(st_within(data_sf,CAgeom)),]
inCA <- data_sf[st_within(data_sf,CAgeom,sparse=FALSE)[,1],]
outCA <- data_sf[!(st_within(data_sf,CAgeom,sparse=FALSE)[,1]),]

outCA
plot(st_geometry(CAgeom)) 
plot(st_geometry(outCA))


