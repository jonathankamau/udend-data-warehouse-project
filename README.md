# Sparkify Data Warehouse Project for song play analysis
The purpose of this project is to build an ETL pipeline that will be able to extract song data from an S3 bucket and transform that data to make it suitable for analysis. This data can be used with business intelligence and visualization apps that will help the analytics team to better understand what songs are commonly listened to on the app.

## Database Schema Design
The project creates a redshift database in the cluster with staging tables that contain all the data retrieved from the s3 bucket and copied over to the tables. They are columnar in nature which helps with parallelizing the execution of one query on multiple CPUs which makes it's peformance much faster for large sets of data as compared to tables in relational databases. Each column on the tables corresponds to the keys in the json files and for the case of the staging_events table, since the column names were different from those in the file, I utilized a json path map file that maps the data elements to the relevant columns.

The project also creates a relational database with a fact and dimension tables, therefore making it a star schema. The reason as to why I utilized star schema as opposed to 3rd normal form is because the star schema is more suitable and optimized for OLAP operations which will be the purpose of the database. This will store data from the staging tables that has been transformed to provide the relevant data in the tables.

## ETL Pipeline
The data gets that gets extracted will need to be transformed to to fit the data model in the target destination tables. For instance the source data for timestamp is in unix forrmat and that will need to be converted to timestamp from which the year, month, day, hour values etc can be extracted which will fit in the target database table schema.

Since the datasets, once copied over to the staging tables, will contain quite a number of columns, the query that inserts the data in the respective tables has a nested query that selects only the relevant columns for the table. In the case of the fact table, there is an added constraint for the data from each staging column that ensures that the values are not null.

## Datasets used
The datasets used are retrieved from the s3 bucket and are in the JSON format. There are two datasets namely `log_data` and `song_data`. The `song_data` dataset is a subset of the the [Million Song Dataset](http://millionsongdataset.com/) while the `log_data` contains generated log files based on the songs in `song_data`.

## Getting Started
In order to have a copy of the project up and running locally, you will need to take note of the following:

### Prerequisites
   - Python 2.7 or greater.
   - AWS Account.
   - IAM role with AWS service as Redshift-Customizable and permissions set to s3 read only access.
   - Security group with inbound rules appropriately set as below:
   
       ```
       Type: Custom TCP Rule.
       Protocol: TCP.
       Port Range: 5439,
       Source: Custom IP, with 0.0.0.0/0
       ```
   - Set your AWS access and secret key in the config file. 
        ```
        [AWS]
        KEY =<your aws key>
        SECRET =<your aws secret>
        ```

### Installation
   - Make a new directory and clone/copy project files into it.
   - Create a virtualenv that will be your development environment, i.e:
       ```
       $ virtualenv sparkify-project
       $ source sparkify-project/bin/activate
       ```
   - Install the following packages in your virtual environment:

            - configparser
            - boto3
            - psycopg2

- Alternatively you can install the requirements in the `requirements.txt` that's in this project by running the command:
    ```
    $ pip install -r requirements.txt
     ```
            
### Terminal commands
- To execute the code that creates the cluster, creates and drops the database tables, run the following command on the terminal:
    ```
    $ python create_tables.py
    ```
 
- After running the above command, the next step is to execute the ETL pipeline which is done by running:
    ```
    $ python etl.py
    ```
- Once done with the cluster, you can delete it by running the following:
    ```
    $ python delete_cluster.py
    ```

## Screenshots of what the final tables look like

### Artists Table
![Artists Table](/assets/images/artists_table.png "Artists Table")

### Songplays Table
![Songplays Table](/assets/images/songplays_table_part1.png "Songplays Table")
![Songplays Table](/assets/images/songplays_table_part2.png "Songplays Table")

### Songs Table
![Songs Table](/assets/images/songs_table.png "Songs Table")

### Time Table
![Time Table](/assets/images/time_table.png "Time Table")

### Users Table
![Users Table](/assets/images/users_table.png "Users Table")

## Built With
- Python and SQL
- AWS Redshift

## Authors
- Jonathan Kamau - [Github Profile](https://github.com/jonathankamau)
