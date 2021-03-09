# ExportToMySQL
A utility to export Cumulus MX monthly and dayfile logs to a MySQL database.

The utility must be run from your Cumulus MX root folder (the folder that holds your CumulusMX.exe file).

It will read various parameters from your Cumulus.ini file, for example the MySQL server name, login details, and table names.

## Command Line Parameters
**monthly** - this will export all the monthly logs ExportToMySQL finds in your /data folder to your MySQL server

**dayfile** - this will your dayfile.txt to your MySQL server

**data/<monthlyfilename>** - this will import a single named monthly log file to your MySQL server
  
 Windows examples:
 
 `> ExportToMySQL monthly`
 
 `> ExportToMySQL dayfile`
 
 `> ExportToMySQL data\Feb21log.txt`


Linux examples:
 
 `> mono ExportToMySQL.exe monthly`
 
 `> mono ExportToMySQL.exe dayfile`
 
 `> mono ExportToMySQL.exe data/Feb21log.txt`
