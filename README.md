# Code test for Data Engineer
The test attempts to convert the weather data into parquet format and prepares it for querying using Athena

![AWS Flow Diagram](https://github.com/GayathriChakravarthy/dlg/blob/main/AWSDataPipeline-DLG.png?raw=true)

## Code Structure by Folders
### Data
Contains two folders, one for CSV files and the other for converted Parquet files
 - csv_files 
 - parquet_files
### Glue
Contains the ETL script used for CSV - Parquet transformation
- **Athena**
Athena queries executed to answer the questions - 
	- Which date was the hottest day?
	- What was the temperature on that day?
	- In which region was the hottest day?
### Lambda
Contains the lambda function called from within S3 (event notification)
This lambda function checks the data quality of the CSV file and runs the Glue catalog crawlers and job as needed. It really is the glue binding all components for this pipeline
## Assumptions
A number of assumptions have been made - 
 - 2 Glue crawlers are used - one for cataloging csv data and the other for the parquet data. These need to be in place to derive schema and associated information
 - 2 Athena queries are used - one to pick data by max(temperature) and the other weighing in the effect of weather code
 - Weather codes information was derived from [this](https://www.metoffice.gov.uk/services/data/datapoint/code-definitions) link
 - Observation Date was modified to date from string
## Improvements
Several improvements can be made to the solution - 
 - Make data checks more robust 
 - Send alert notifications from within lambda using SNS
 - Chain lambda functions for success / failure events and to keep code modular
 - Partitions can be introduced to optimise Athena and the partitions can be processed (MSCK Repair Table) from Lambda
