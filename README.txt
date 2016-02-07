Asad Ali
MPCS53013
Big Data
Fall Quarter 2015

Final Project

Web App: 
http://104.197.20.219/acidreflux/crime/crime.html

Intro/Data
**********
The dataset I chose was the City of Chicago's crime reports from 2001 to present (https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). This came in the form of a singe large CSV file, and is updated daily. Luckily, there is a separate view that only lists the current year (https://data.cityofchicago.org/Public-Safety/Crimes-One-year-prior-to-present/x2n5-8w5q), from which I sourced my new data stream.

The app I made allows the user to see how many of each type of crime (defined by the types they list under "PRIMARY DESCRIPTION") occur in a given ward in a given month of the year, on average.

Batch Layer
**********
The java project "serialCrime" serializes the input from the large CSV file and uploads it to hdfs. readCrimeData.pig puts all of the data in to a pig table.

Serving Layer
*************
The data from pig is loaded into an hbase table called "acidreflux_crime_data" with the column families "year" (to track the most current year in which the data was collected) and "crime" (to track the number of occurrences of each type of crime). Rows are keyed by the month of the year and the ward, concatenated with a hyphen between them. The script "doCrimeToHbase.pig" puts all the data into the hbase table, with each type of crime sorted under its own column in "crime", summed with 1's where the crime is of that type, and 0's where it isn't, similarly to the various weather delays with the example project.

The web app can be found at http://104.197.20.219/acidreflux/crime/crime.html. The html, css, cgi, and perl for it can be found under the "serving" folder here.

Speed Layer
***********
The "kafka-crime" java project loads data in to the kafka queue ("acidreflux-crime-events") from the updated csv file daily. Since the file contains a ton of redundant data, the java script only reads data for the most recent date. Since the CSV file is also listed from oldest to newest by default, it downloads from an alternative view of the file that reverses the order.

The "reference-topology" java project is the storm topology for reading messages from my kafka queue and updating the hbase table. It basically just checks the month, year, ward, and type, and increments the type according to the type pulled from the message. If the year pulled from the message is higher than the max year in the relevant row of the hbase table, it updates that year as well.
 
The kafka updater is currently running on the cluster.


