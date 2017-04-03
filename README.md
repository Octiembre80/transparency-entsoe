# Transparency-entsoe

Warning: Unfinished code, sample code for kiwi internship

Simple tool to download and repair some of the data from transparency.entsoe.eu API platform. 

### Usage
``` 
python main.py
```
### Parameters:
* --data_type - specification on data type
  allowed 'generation-update','generation-forecast','transmission-update','load-actual'
* --days_back - How many last day to update (eg. 1 means since tomorrow)
* --start_date - update every day from start_date until today format: Y-m-d

