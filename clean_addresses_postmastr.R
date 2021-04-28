library(dplyr)
# library(magrittr) # necessary for %<>%
library(postmastr)

# 0: LOAD DATA
setwd("T:/Home/Shantanu/intermediate_data")
nem <- read.csv("PGE_Master_DetailsRemoved.csv",stringsAsFactors=F) # interconnections
addr <- "Service.Address"

original_df <- nem %>%
  pm_identify(var=Service.Address)
parsed_df <- original_df %>% 
  pm_prep(var=Service.Address,type="street") %>% # 1.2: minimal postmastr object
  # (2, 3, 4: Postal Codes, States, & Cities)
  pm_house_parse() %>% # 5: House Numbers
  pm_streetDir_parse() %>% # # 6.1: Street Directionals (Prefix & Suffix)
  pm_streetSuf_parse() %>% # 6.2: Street Suffix
  pm_street_parse(ordinal=TRUE,drop=TRUE) %>% # 7: Street Names
  pm_replace(source=original_df) # 8.1: add parsed data back into dataframe

# manually determine end for pm_rebuild
# highest level of postmastr workflow in our addresses
pm_maxlevel <- names(parsed_df)[max(grep("pm\\.",names(parsed_df)))]
# https://github.com/slu-openGIS/postmastr/blob/master/R/build.R
# see code labeled "determine end" w/ "if(output=="short){"
endQ <- rlang::quo(!! rlang::sym(pm_maxlevel))
rebuilt_df <- pm_rebuild(parsed_df,output="short",side="left")

# merge datasets together
merge(x=res,y=rebuilt_df,
         by.x=c("PREM_ADDR","PREM_CITY","PREM_ZIP5"),
         by.y=c("pm.address","City","Zip"))

nem1 %<>% pm_identify(var="addr") %>% # 1.1: pm.id, pm.uid for each row
  pm_prep(var="addr",type="street") %>% # 1.2: minimal postmastr object
  # (2, 3, 4: Postal Codes, States, & Cities)
  pm_house_parse() %>% # 5: House Numbers
  pm_streetDir_parse() %>% # # 6.1: Street Directionals (Prefix & Suffix)
  pm_streetSuf_parse() %>% # 6.2: Street Suffix
  pm_street_parse(ordinal=TRUE,drop=TRUE) %>% # 7: Street Names
  pm_replace(source=nem1) -> parsed # 8.1: add parsed data back into dataframe

# manually determine end for pm_rebuild
# endQ = highest level of postmastr workflow in our addresses?
names(parsed)[length(names(parsed))]
# https://github.com/slu-openGIS/postmastr/blob/master/R/build.R
# see code labeled "determine end" starting with "if(output=="short){"
endQ <- rlang::quo(!! rlang::sym("pm.sufDir.y"))
recombined <- pm_rebuild(parsed,output="short")

