# PythonEDA
Project for the LVL 5 Data Engineering Course


The pipeline serves to solve a problem the library is having. The problem is that all cleaning is done manually taking an incredible amount of time to complete thus taking time away from serving the libraries customers lowering the customer service that the library can offer.

We look to solve this problem creating a simple csv cleaning service utilising python. This python application will be simple in its use as to not take away additional service time from the library operatives.

We can further expand on this project further by implementing data storage capabilities and further developments can implement graphical user interfaces for both data ingestion and data extraction and package the product as a standalone system through docker

We achieve the run time on this python app through 3 main functions. 
One python function will clean the customer data ensuring that all null fields are handled, the data is free from duplicates and any erroneous entries are cleansed.

The second function looks to clean the data from the rental system ensuring date formating and validity are considered.

This is before both of these functions produce a cleaned CSV in a cleaned folder whilst retaining a timestamped historical version for recording purposes.

The third function produces pipeline metrics allowing us to monitor the runtime, the intial rows, the dropped rows as well as the date that the pipeline was started and stopped before exporting that data to a csv for visualisation through powerbi.

The whole pipeline is then orchestrated through a central python file called main which calls each of these functions into action.

