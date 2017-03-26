# University-Database
John Nguyen 998808398
Eric Du  913327304 

Use python3 to run insert.py
Type in the directory that the .csv files are in. 
The program will read all the files that end in .csv in that directory so if you need to remove the .csv
that you don't want to read outside of that directory

For example:
python3 insert.py
Enter directory: /Users/John/Desktop/Grades


Use python3 for queries.py
The output contains the answers to all the questions in problem 3

EXTRA CREDIT:
For 5a: 
Use python3 to run extra_credit.py 
Make sure the file is in the same directory as the other files
This file is the extra credit query for number 5 

For 5b:
Use python3 to run expand.py
The program assumes that the database has already been created and there are data in there
CID and term must exist in the table 
For example:
What is the CID? 96273
What is the term? 198906
How many students would you like to add? 100


Resources used:
To help create the tables
http://www.postgresqltutorial.com/postgresql-python/create-tables/

To help break the data up by the white line
Functions group_by_heading() and process_file() are from this site 
http://stackoverflow.com/questions/4595197

To help with the syntax of converting time
http://stackoverflow.com/questions/1759455

Used the same logic to compare overlap
http://stackoverflow.com/questions/17106670

Syntax for batch insert
http://stackoverflow.com/questions/8134602
