# Understanding the Role of Electric Power Outages on the Adoption of Solar and Storage Technologies
## Shantanu Kadam, under [Will Gorman](https://sites.google.com/view/whgorman/home) and [Duncan Callaway](https://erg.berkeley.edu/people/callaway-duncan/) 
## Spring 2021 [SPUR](https://nature.berkeley.edu/undergraduate-research/spur/) Project
### Project Description
Solar adoption plays a key role in questions of clean energy, carbon goals, and load/grid defection. Home solar installations are increasing, in part due to declining technological costs, increasing climate awareness, and political support. One economic incentive is Net Energy Metering (NEM), which allows consumers producing electricity to mitigate costs by storing excess electricity on the grid.

In recent years, California's drought and wildfires have spurred utility companies to enact preemptive power outages. Take, for example, PG&E's Public Safety Power Shutoff (PSPS) events. The resulting power uncertainty is an additional incentive for homeowners to install solar. 

We seek to answer the question "How do electric power outages influence the solar installation trends?" My work this semester focused on visually and computationally identifying trends in NEM installations that may have been influenced by 2019 PG&E PSPS events while also developing and standardizing procedures for address parsing and geocoding.

### Workflow 
#### Datasets
1. home solar installations in California 
2. 2019 PG&E PSPS events _hyperlink_

The former contains addresses where solar was installed. The latter contains spatial information on the electricity feeders which were turned off during PSPS events. Some feeders were unaffected by PSPS events in 2019, whereas others were affected. If we can identify which addresses are in which feeders, then we can identify differences in the quantity of solar installed and determine whether those differences are due to PSPS events. I call this process of identifying an address (which is point data) by the feeder it lies in (which is a polygon contained in a geodatabase layer) "point-in-layer" analysis. 

Two other datasets provide additional information for my analysis: power outage duration across California _hyperlink_ and geographies for all PG&E feeders (specifically those unaffected by outages).

#### Research Steps
These processes are common and useful for many geospatial objectives, which is my motivation for the semi-generalized code in this repository. 
1. Address Parsing and Standardization
   * Locations where solar was installed are documented. Because of human error they are not always correct. Furthermore, different geocoding APIs may accept different abbreviations. 
3. Geocoding
   * Associate each address with a longitude and latitude by passing the address to a geocoding API
5. Points-in-Layer 
   * identify the feeder to which each address belongs (i.e. what feeder supplies power to each house?)

#### How to Use the Scripts
_insert workflow pictures w/ script names_

### Scripts (include function purposes?)
_Scripts in italics were not used in my final procedure (and so are not as well-annotated), but are included for additional detail._
| Script | Purpose |
| --- | ---|
| _clean_addresses_postmaster.R_ | Step-by-step use of `postmastr` to parse and standardize addresses |
| geocoding_functions.R | Functions for batch geocoding. |
| geocoding_example.R | Example workflow for using geocoding_functions.R |
| points_in_layer.R | Identify whether point data lie within geographic regions |
| _points_in_layer_event.R_ | Similar to above, but apply several different geographic regions sequentially. |

#### Caution 
* geocoding_functions.R uses the parallel processing option in `censusxy::cxy_geocode`. 
* geocoding_functions.R has slight variability in which addresses get geocoded. Currently trying to rectify this issue. Consider running the any resulting, ungeocoded addresses through another geocoding process (either manual or this script again). 

#### Comments on Code
* _clean_addresses_postmastr.R_ is only useful if you wish to look "behind the hood" of the `postmastr` procedure. See _points_in_layer.R_ for an example of the more practical `postermastr::parse`.
* Use _points_in_layer.R_ if the relevant polygon geometries (e.g. feeders) are all contained in one file. Use _points_in_layer_event.R_  when you need to run a points-in-layer analysis using the geometries from one file, remove all identified points, and repeat the process for each file's geometries sequentially.
   * If relevant polygon geometries are split between many files but the points-in-layer analysis does not need to be done indidivually for each file, consider merging all of the polygon geometries using the following code. 
   *  ```
               # read in all files 
               # merge shapefiles 
               # any other processing? 
               # save result  
      ```
