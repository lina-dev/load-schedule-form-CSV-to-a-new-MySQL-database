The uploading data from CSV files was developed for analysis of mycelium leather growing . This data  was estimated for experiments with different growth schedules.
The company had 2 growth programs in use (described in runcard_A.csv, runcard_B.csv), which described a several day process. Both steps started with 'Load' and end with 'Harvest'. A current dump of production was in given file loads.csv, which kept the program type (runcard_A/B), the date of the load, and an identifier for the production. Based on this information I developed an architecture for uploading data from the CSV files and building reports to analytics group about required tasks which much be completed on a given day.
To normalize storing data in MySQL database I created  tables Processes, Runcard, Schedule_Runcard, Loads and put the relevant data into tables. This code returned lists of required tasks for the day for analytics group.

 
