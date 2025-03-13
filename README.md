# erddap_export_workflow


This repo contains various templates for pulling data off OpenDap Servers (ERDDAP specifically)





# future potentials

By formatting workflows to take inputs via config parameters provided by the users, workflows can be reformatted to pull data from a variety of environmental data sources with additional configuration for additional sources (ex: erddap, opendap, globus api, etc)



outline: 

modules and objects:

config
- yaml based configuration file that users can edit 
- abstract enought to be applied to a variety of geospatial/environmental data platform interfaces in unison with the parse_config module

data_transfer 
- handles formatting configuration arguments into a the correct syntax for importing from remote erddap compatible servers
- includes cases for importing from opendap
- modularity potential: can be integrated and updated to format additional arguments 


authentication 
- handles authentication for erddap data

parse config
- parses input configuration file into a config object holding information necessary for pulling data
- validates data types contained in the configuration against the variables.yaml
- can be split into two different modules: parsing and validation
- modularity potential: can be integrated into other API compatible applications to supply information


issues: 
- firewall and security issues in pulling data via automation 




