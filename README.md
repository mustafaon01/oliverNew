# 'XML_parser.py'
## Overview
This project is designed to streamline the process of parsing XML files, loading the parsed data into a database, and normalizing the database tables according to specific requirements. By automating these tasks, the project aims to save time and reduce errors in data handling and database management.

## Components

### Core Components ###
- BaseXMLParser: Serves as the abstract base class for specific XML parser implementations. It establishes a common interface for all parsers, requiring the implementation of methods for data extraction and handling. This class also includes utility methods for database interactions, such as loading data into the database and managing database tables.

- EditorXMLParser and StateXMLParser: These classes extend BaseXMLParser, providing specific implementations for parsing different types of XML files. They override abstract methods to extract and process data unique to their respective XML structures, demonstrating the application's modularity and ability to adapt to various data formats.

- NormalizerUtils: A utility class designed to normalize data, ensuring that the database schema is optimized for efficient queries. It processes DataFrame objects to identify shared fields across different data tables, creating a normalized structure that eliminates redundancies and improves data integrity.

### Data Flow ###
- Initialization: The application initializes by setting up the database connection through SQLAlchemy and preparing the database schema for data loading. It employs the automap_base function to dynamically reflect the current state of the database schema, allowing for a flexible and adaptable architecture.

- XML Parsing: Upon receiving XML files, the application utilizes xml.etree.ElementTree for parsing. The EditorXMLParser and StateXMLParser classes handle specific XML structures, extracting data and converting it into Pandas DataFrame objects for easier manipulation and analysis.

- Data Extraction and Transformation: Extracted data may undergo various transformations to align with the database schema or to fulfill specific normalization requirements. This process includes converting XML data into structured formats, applying necessary data conversions, and preparing data for database insertion.

- Database Loading and Normalization: DataFrames are then loaded into the PostgreSQL database using SQLAlchemy. The NormalizerUtils class plays a crucial role in this step, analyzing the data for normalization opportunities. It adjusts the data structure to optimize database performance and ensure data integrity.

- Database Management: The application includes methods for managing the database schema, such as creating or dropping tables, reflecting existing tables, and modifying table structures to accommodate the parsed data. This functionality allows for dynamic schema management, adapting to changes in the data or requirements over time.

## asdasd

- Dynamic Database Schema Management: Utilizes SQLAlchemy's reflection capabilities to dynamically interact with the database, allowing the application to adapt to changes in the database schema without requiring manual updates to the code.

- Modular Design: The application's architecture is highly modular, with separate classes handling different aspects of the XML parsing and data management process. This design makes it easy to extend the application to support new XML structures or database schemas.

- Efficient Data Handling: Combines the power of SQLAlchemy for database operations with Pandas for data manipulation, creating a highly efficient pipeline for processing and managing XML data.


## Usage

```sh 
git clone (git repo link)
```

```sh 
cd  (repo name)
```

```sh 
pip install -r requirements.txt 
```

```sh 
cd python app.py 
```

# 'postgres_connect.py'

## Overview ##
This utility script, facilitates seamless interaction with a PostgreSQL database. It leverages SQLAlchemy for database connection and operations, automating tasks such as table reflection, data retrieval, insertion, deletion, and more. The script is structured to enhance ease of use, with functionality encapsulated within a PostgresConnect class. Features include dynamic table access, data conversion to dictionaries for easy manipulation, and utility functions to simplify common database operations.

## Features ##
- Dynamic Parameter Handling with **kwargs: Functions in the script use **kwargs to accept an arbitrary number of keyword arguments, making the utility highly adaptable to various database operations without needing method signature changes.
- Automated Table Reflection: Dynamically reflects the database schema to access and interact with tables as Python objects.
- Data Manipulation: Supports essential CRUD (Create, Read, Update, Delete) operations, making database management straightforward and efficient.
- Mixin Utility: Includes a ToDictMixin mixin for converting SQLAlchemy model instances into dictionaries, facilitating easier data handling and manipulation.

## Usage ##

### Initialization ###
- Instantiate the PostgresConnect class to start interacting with your database:

```python
from postgres_connect import PostgresConnect

postgres_connect = PostgresConnect()
```

### Reflecting Tables and Retrieving Data ###
- Reflect the database tables and retrieve data from a specified table:

```python
# Reflect tables and print their names
postgres_connect.print_exist_tables()

# Retrieve all records from the 'Project' table
projects = postgres_connect.get_selected_table('Project')
print(projects)
```

### Inserting Data ###
- Insert data into a specified table:

```python
postgres_connect.insert_data_to_selected_table(
    table_name='Project', 
    Project_ID='python_test_project', 
    ProjectName='python_test_name'
)
```

### Deleting Data ###
- Delete data from a specified table:

```python
postgres_connect.delete_data_from_selected_table(
    table_name='Project', 
    Project_ID='python_test_project'
)
```
### Filtering Data ###
- Retrieve data from a table based on specific criteria:

```python
filtered_projects = postgres_connect.get_filtered_data_from_selected_table(
    table_name='Project', 
    ProjectName='python_test_name_2'
)
print(filtered_projects)
```

## Extending Functionality ###
- The PostgresConnect class can be extended to include more complex operations, such as joining tables, complex filters, or transaction management, by adding new methods and utilizing the power of SQLAlchemy.


