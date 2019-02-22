# OCN_QUALI_HMS
===============

# Using HMS_QA.py  
Python 2/3 compatible code.
This script create a log.dat that contain information about HMS files.  
Looping all UCDs that contain HMS.  

# Using HMS_QA.json
json file to configure HMS_QA.py

# Using HMS_bd.py  
Python 2/3 compatible code.  
This script create a DataBase in SQLITE3  
(Class CheckBd) and populate with a range of data (Class AttitudeData).  


# Using HMS_bd.json
json file to configure HMS_bd.py

# Using HMS_QC.py  
Python 2/3 compatible code.  
This script connect with DataBase (SQLITE3) (Class ReadDB)  
(Class HMS_QC) allow plot HMS data.  

TODO: improve efficiency  
      create method to qualify data  
      create index of quality  
      create better graphs  

# Using HMS_QC_gui.py  
Python 2/3 compatible code.  
Graphical User Interface.
Alow atitude data plots .  
