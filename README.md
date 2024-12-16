
# NYC TLC Data Pipeline

## Pipeline Steps

### 1. Extract 


The pipeline downloads monthly TLC datasets for Yellow and Green taxis from the official TLC repository. Files are available on the website as Parquet files named in the format `yellow_tripdata_YYYY-MM.parquet` and `green_tripdata_YYYY-MM.parquet`. 

**Question:** The input data is spread over several files, including separate files for “Yellow” and “Green” taxis. Does it make sense to merge those input file into one?
*I merged them into one and added a new field `taxi_type` to differentiate between the two.*

**Question:** You will notice that the input data contains “date and time” columns. Your colleagues want to evaluate data also on hour-level and day of week-level. Does that affect your output-structure in some way? *I converted timestamps and added `pickup_hour` and `pickup_dayofweek` to make time-based analysis easier.*

### 2. Transform


- Standardize column names (`pickup_datetime`, `dropoff_datetime`).
- Added derived columns: `pickup_hour`, `pickup_dayofweek`.
- Addded a `taxi_type` column to combine Yellow and Green data under a single schema.
- To ensure data quality, 


### 3. Load 


All monthly processed Parquet files are combined into a single unified Parquet file, `all_taxi_data.parquet`. 

And then into Avro format.
*Avro is a row-oriented format suitable for certain downstream systems or integrations.* It uses `fastavro` and a predefined Avro schema, `avro_schema.json`.


### Queries

I used DuckDB to run SQL queries directly on the Parquet file for correctness and exploration:

- Average trip distanc by taxi type and hour
- Day of week with the lowest single rider trips in 2019 & 2020
- Top 3 busiest hours

**Queries (SQL):**
```sql
-- avg trip distance by taxi type and hour
SELECT taxi_type, pickup_hour, AVG(trip_distance) AS avg_distance
FROM trips
GROUP BY taxi_type, pickup_hour
ORDER BY taxi_type, pickup_hour;

-- day of the week in 2019 and 2020 with lowest singel rider trips
SELECT pickup_dayofweek, COUNT(*) AS single_rider_trips
FROM trips
WHERE passenger_count = 1 AND EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
GROUP BY pickup_dayofweek
ORDER BY single_rider_trips
LIMIT 1;

-- top 3 busiest hours
SELECT pickup_hour, COUNT(*) AS trip_count
FROM trips
GROUP BY pickup_hour
ORDER BY trip_count DESC
LIMIT 3;
```

## Output & Schema

- `all_taxi_data.parquet`: column-oriented dataset
- `all_taxi_data.avro`: row-oriented dataset
- `all_taxi_data.csv`: for my excel guru colleague. For this run `python script.py --export-csv` after the final Parquet file is created. Because of the size of dataset, I used CSV format due to Excel’s row limits.

**Schema:**


| Column Name           | Data Type          | Description                           |
|-----------------------|--------------------|---------------------------------------|
| VendorID              | int64              | A code indicating the TPEP provider that provided the record             |
| store_and_fwd_flag    | object (string)    | Indicator if the trip was held in memory before sending to the vendor |
| RatecodeID            | float64            | Numeric code signifying the rate used |
| PULocationID          | int64              | TLC Taxi Zone in which the taximeter was engaged              |
| DOLocationID          | int64              | TLC Taxi Zone in which the taximeter was disengaged|
| passenger_count       | float64            | Number of passengers in the vehicle   |
| trip_distance         | float64            | Distance traveled during the trip     |
| fare_amount           | float64            | Fare amount charged for the trip      |
| extra                 | float64            | Miscellaneous extras and surcharges |
| mta_tax               | float64            | MTA tax added to the trip fare        |
| tip_amount            | float64            | Tip amount paid by the passenger      |
| tolls_amount          | float64            | Total amount of tolls paid            |
| ehail_fee             | object (string)    | e-Hail fee    |
| improvement_surcharge | float64            | 30 cent improvement surcharge         |
| total_amount          | float64            | Total amount paid (fare + extras)     |
| payment_type          | float64            | Payment method code (e.g., credit card) |
| trip_type             | float64            | Code indicating trip type (e.g., street hail vs. dispatch) |
| congestion_surcharge  | float64            | Additional fee for trips in congested areas |
| pickup_datetime       | datetime64[us]     | Date and time when the trip started   |
| dropoff_datetime      | datetime64[us]     | Date and time when the trip ended     |
| pickup_hour           | int32              | The hour of day (0-23) of pickup time |
| pickup_dayofweek      | int32              | Day of the week for pickup (0=Monday, 6=Sunday) |
| taxi_type             | object (string)    | Type of taxi (e.g., "yellow", "green")|
| airport_fee           | float64            | Additional airport fee |



**Avro Schema (avro_schema.json):**
```json
{
    "type": "record",
    "name": "trip_record",
    "fields": [
      ...
      { "name": "passenger_count", "type": ["null", "double"] },
      { "name": "trip_distance", "type": ["null", "double"] },
      { "name": "fare_amount", "type": ["null", "double"] },
      { "name": "extra", "type": ["null", "double"] },
      { "name": "mta_tax", "type": ["null", "double"] },
      { "name": "tip_amount", "type": ["null", "double"] },
      { "name": "tolls_amount", "type": ["null", "double"] },
      { "name": "ehail_fee", "type": ["null", "string"] },
      { "name": "improvement_surcharge", "type": ["null", "double"] },
      { "name": "total_amount", "type": ["null", "double"] },
      { "name": "payment_type", "type": ["null", "double"] },
      { "name": "trip_type", "type": ["null", "double"] },
      { "name": "congestion_surcharge", "type": ["null", "double"] },
      { "name": "pickup_datetime", "type": ["null", "string"] },
      { "name": "dropoff_datetime", "type": ["null", "string"] },
      { "name": "pickup_hour", "type": "int" },
      { "name": "pickup_dayofweek", "type": "int" },
      { "name": "taxi_type", "type": ["null", "string"] },
      { "name": "airport_fee", "type": ["null", "double"] }
    ]
}
```
I ommited the some Id fileds from the avro schema above for demonstration purposes.  

## Power BI 
I then created a Power BI dashboard. I incorporated the queries and insights I developed earlier in this dashboard, and I also created calculated measures and added new columns using DAX. 

  - **Measures:**  DAX measures for average trip distance, total trips, and single-rider trip counts. 
  - **Day of Week Column:** DAX calculated column to convert the numeric day-of-week values into friendly names (e.g., "Monday", "Tuesday"). This made the  `Number of single rider trips in 2019 and 2020` visual more intuitive.
  
![Dashboard](/resources/venturedata_tlc_de_task-1.png "a title")
## Run and Deploy the Pipeline

 

1. Clone the repository using the terminal. Inside the terminal, run 
   ```
   git clone https://github.com/t-iuy987/venturedata_task.git
   ```
   A folder called "venturedata_task" will be created on your current folder. 

2. It's a good idea to create a virtual environment prior to installing all the dependencies. This can be done again using the terminal by running 
   ```
   python -m venv .venv
   ```
3. Activate the virtual environment using the terminal:
   ```
   source .venv/Scripts/activate
   ```
   The packages that the script makes use of can be install via pip:
   ```
   pip install -r requirements.txt
   ```
4. Start the script using the terminal (as you would any other Python file)
   ```
   python3 script.py
   ```


## Automation
I set up my pipeline to run automatically each month on WSL, making sure I only download new data.

**According to the guide on TLC website, *The TLC currently updates trip records every six months, so you should expect files for January-June by the end of August and July-December by the end of February.*
[Source](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf)**

### Environment
First, I'm running on WSL (Windows Subsystem for Linux), so I have a Linux-like environment on my Windows machine. 
### Avoiding re-downloads
To avoid re-downloading old data, I keep track of what I've already processed. After the pipeline finishes downloading and processing a given month, it writes year and month in the format `YYYY-MM` to `last_downloaded.txt`. Next time it runs, it reads this file to figure out what months to process next. 

### Scheduling with Cron
I used Cron for setting schedules on Linux.

1. **Install and Start Cron**  
   I made sure cron is installed and running:
   ```bash
   sudo apt-get update
   sudo apt-get install cron
   sudo service cron start
   ```

2. **Edit the Crontab**  
   The data is released every six months according to the TLC’s schedul:
   ```bash
   crontab -e
   ```
   and then added this line:
   ```bash
   0 2 1 9 * /usr/bin/python3 /mnt/c/Users/murva/Documents/venturedata_task/script.py >> /mnt/c/Users/murva/Documents/venturedata_task/logs/pipeline_sept.log 2>&1

   0 2 1 3 * /usr/bin/python3 /mnt/c/Users/murva/Documents/venturedata_task/script.py >> /mnt/c/Users/murva/Documents/venturedata_task/logs/pipeline_mar.log 2>&1
   ```

  
## References
1. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf
3. https://www.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf
4. https://fastavro.readthedocs.io/en/latest/
5. https://duckdb.org/docs/api/python/data_ingestion
6. https://crontab.guru/
7. https://learn.microsoft.com/en-us/power-bi/transform-model/desktop-measures
8. https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-add-custom-column
