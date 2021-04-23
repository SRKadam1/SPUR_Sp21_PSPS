# Understanding the Role of Electric Power Outages on the Adoption of Solar and Storage Technologies
## Shantanu Kadam, under [Will Gorman](https://sites.google.com/view/whgorman/home) and [Duncan Callaway](https://erg.berkeley.edu/people/callaway-duncan/) 
## Spring 2021 [SPUR](https://nature.berkeley.edu/undergraduate-research/spur/) Project
### Project Description
### Description of Workflow (How to Use the Scripts)
### Description of Scripts (file name, function name, purpose)
_scripts in italics were not used in my final procedure, but are included for additional detail._
| Script | Purpose |
| --- | ---|
| _clean_addresses_postmaster.R_ | Step-by-step use of `postmastr` to parse and standardize addresses |
| geocoding_functions.R | Functions for batch geocoding. |
| geocoding_example.R | Example workflow for using geocoding_functions.R |
| points_in_layer.R | Identify whether point data lie within geographic regions |
| _points_in_layer_event.R_ | Similar to above, but apply several different geographic regions sequentially. |

* Add in 
    * should not use clean_addresses_postmastr unless you really want a look behind the hood (`parse`) is better. Refer to points_in_layer for use there. 
    * points_in_layer: what is a layer? 
    * justify points_in_layer_event.R (why is this advantageous in some situations, such as when you have outage feeder info but not all feeders)
        * not advantageous when you have multiple files (just rbind or data.table join them all together) 
            * code for how to do so here... 

### Accessing Data
(especially the publicly available data)
