Hi,
Before explaining the project approach, I would like to give a short intro.
Simply, I have been for a while away of having similar challenge/opportunity to have a logical thinking and solve problems + writing code. I enjoyed every single challenge and every task - although it took much time in comparison to my expectations finding the correct answers and logic behind each + learning as well. Happy and motivated to be part of this transformation and learning data engineering using latest technologies.

##My Approach:
    Simply, I followed the project Workspace steps and templates. The reason for that, I have technical limitation to install all needed tools and framework within my work laptop. Beside that, I was in a need for a guide within each step - I was able to find that within the workspace explained steps and the knowledge center.
    While going through project steps, I was in a need to learn from others and most of the points I was looking for, I was able to find within the Knowledge center in relation to my stream and project.


##Purpose of the Database:
    Simply this Database present structured view of Sparkify available data. The main objective of building this DB as well to support future analysis and structured view regarding Sparkify customer usage of songs. As of today, the main need of Sparkify to have deeper understanding of customer's usage of songs and which songs the users listening to.
    
##How to run Python Scrips:
- Open Terminal ==> Use the workspace to initiate new terminal session
- first, need to create the database using this command ==> python3 create_tables.py
- Then, need to run the ETL process python file using this command ==> python3 etl.py

Each command will call for the relevant python files and run related code within each file

##Explanation of the files in the repository:
    - File "create_tables.py" ==> this file covers the main functions to create Database and drop/create tables related in connection with file "sql_queries.py"
    - File "sql_queries.py" ==> this file contains the main commands of dropping tables (in case exist) + create new tables + insert command and related parameters (to support other functions execution)
    - File "etl.py" ==> This file contain the main functionality of reading the files (in loop) and do the ETL activities till insert the data within relevant tables
    - File "test.ipynb" ==> This file accountable to run test cases to evaluate the project steps and data base creation and data transformed and inserted in the new tables
    
    
 
   
