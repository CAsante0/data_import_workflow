# Data Import Workflow

This library automates data retrieval using a user-supplied configuration file, making it easier to pull predefined datasets without manual work. The configuration file specifies which platform to pull data from (ERDDAP, OPeNDAP, or GlobusAPI), what parameters to use, and where to store the results.

Once the system reads the configuration file, it initializes the correct data import class and runs the retrieval workflow. The data is then formatted and saved, ready for use in an existing processing pipeline or repository. This setup allows users to automate recurring data pulls, keeping datasets up to date for analysis. By handling platform-specific details behind the scenes, this module reduces time spent on data acquisition and ensures consistency across updates.

# Class Structure and Design



## Modules:

### config
- yaml based configuration file that users can edit 
- abstract enought to be applied to a variety of geospatial/environmental data platform interfaces in unison with the parse_config module

### util/data_transfer.py
The ImportDatasets class defines a base structure for retrieving and formatting datasets from a variety of data platforms. Child classes implement platform-specific workflows to ensure efficient data access, making the ImportDatasets class easily expandable to a variety of workflows.

Child Classes:
### ERDDAP
Interfaces with ERDDAP servers to retrieve structured oceanographic and environmental data. Supports RESTful and ERDDAP -compliant queries.
File Types Retrieved: Griddap, tabledap, image

### OPeNDAP (planning stage)
Accesses datasets via the OPeNDAP protocol, optimizing data retrieval analysis.
File Types Retrieved:

### GlobusAPI (not started)
Facilitates secure and automated data transfers across distributed storage systems using the Globus platform.
File Types Retrieved:
Each child class extends ImportDatasets to implement workflows tailored to its respective data source.


## util/authentication.py
- handles authentication 

## util/parse_config.py
- parses input configuration file into a config object holding information necessary for pulling data
- validates data types contained in the configuration against the variables.yaml
- can be split into two different modules: parsing and validation
- modularity potential: can be integrated into other API compatible applications to supply information


## current_issues: 
- firewall and security issues in pulling data via automation 




