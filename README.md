The load from CSV files was developed for growing analisys of mycelium leather . The company's team is experimenting with
different growth schedules.

They have 2 growth programms in use (described in runcard_A.csv, runcard_B.csv), which describe a several day process.
Both steps start with 'Load' and end with 'Harvest'. In between are steps called 'Colonize' which is a passive step,
and 'Fertilize', which is where the operators provide nutrients to the growth. A current dump of production loads
is given in loads.csv, which lists the program type (runcard_A/B), the date of the load, and an identifier for
the production.
Based on this I developed a architecture for uploading data from the CSV files and building reports to analytics group about actual tasks which much be completed on a given day.
