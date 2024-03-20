from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

import os

''' load env variables from .env file to use in the code '''
load_dotenv()

''' get env variables to connect to the database '''
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_PORT = os.getenv('DB_PORT')

''' create the engine url to connect to the database '''
engine_url = f'postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'


class ToDictMixin:
    def to_dict(self):
        """
        Convert the attributes of the model instance to a dictionary.
        """
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}
    

class PostgresConnect:
    def __init__(self, engine_url=engine_url):
        self.sql_engine = create_engine(engine_url, pool_size=10, max_overflow=20, pool_timeout=30)
        self.Base = self.initialize_base()
        self.Session = sessionmaker(bind=self.sql_engine)

    ''' initialize the base object to use in the code '''
    def initialize_base(self):
        Base = automap_base()
        Base.prepare(self.sql_engine, reflect=True, schema='public')
        self.apply_mixin(Base)
        return Base
    
    ''' 
    Apply the ToDictMixin to the classes in the Base object, so that we can use the to_dict method for each class(=table)
    '''
    def apply_mixin(self, Base):
        for class_ in Base.classes:
            class_.__bases__ = (ToDictMixin,) + class_.__bases__
    
    '''
    Print existing tables from the database
    '''
    def print_exist_tables(self):
        print(self.Base.classes.keys())
    
    '''
    Get selected table from the database as dict in a list
    param: session: a session object
    param: table_name: the name of the table to query
    return: a list of dictionaries, each dictionary represents a row in the table
    '''
    def get_selected_table(self, table_name):
        try:
            session = self.Session()
            table = getattr(self.Base.classes, table_name)
            results = session.query(table).all()
            return [result.to_dict() for result in results]
        except Exception as e:
            print(f"Exception occurred while getting the table for {table_name}: {e}")
        finally:
            session.close()
    
    ''' 
    Get filtered data from the selected table as dict in a list
    retrun: a list of dictionaries, each dictionary represents a row in the table
    '''
    def get_filtered_data_from_selected_table(self, **kwargs):
        try:
            session = self.Session()
            table_name = kwargs.pop('table_name')
            table = getattr(self.Base.classes, table_name)
            results = session.query(table).filter_by(**kwargs).all()
            return [result.to_dict() for result in results]
        except Exception as e:
            print(f"Exception occurred while getting the table for {table_name}: {e}")
        finally:
            session.close()
    
    '''
    Insert data into a table that is selected from the database    
    '''
    def insert_data_to_selected_table(self, **kwargs):
        try:
            session = self.Session()
            table_name = kwargs.pop('table_name')
            table = getattr(self.Base.classes, table_name)
            new_row = table(**kwargs)
            session.add(new_row)
            session.commit()
            print(f"Data inserted successfully into {table_name}. With data: {kwargs}")
        except Exception as e:
            print(f"Exception occurred while inserting data to the table for {table_name}: {e}")
        finally:
            session.close()
    
    '''
    Delete data from a table that is selected from the database
    '''
    def delete_data_from_selected_table(self, **kwargs):
        try:
            session = self.Session()
            table_name = kwargs.pop('table_name')
            table = getattr(self.Base.classes, table_name)
            session.query(table).filter_by(**kwargs).delete()
            session.commit()
            print(f"Data deleted successfully from the table for {table_name}. With data: {kwargs}")
        except Exception as e:
            print(f"Exception occurred while deleting data from the table for {table_name}: {e}")
            session.rollback()
        finally:
            session.close()


if __name__ == '__main__':
    ''' 
    create the PostgresConnect object 
    '''
    postgres_connect = PostgresConnect()

    '''
    All the methods are tested below
    '''

    print(postgres_connect.get_selected_table('Project'))
    postgres_connect.insert_data_to_selected_table(table_name='Project', Project_ID='python_test_project', ProjectName='python_test_name')

    '''
    We don't delete this data for testing purposes. However, we need to delete it after testing to get back to the initial state.
    '''
    postgres_connect.insert_data_to_selected_table(table_name='Project', Project_ID='python_test_project_2', ProjectName='python_test_name_2')
    postgres_connect.delete_data_from_selected_table(table_name='Project', Project_ID='python_test_project', ProjectName='python_test_name')
    print(postgres_connect.get_selected_table('Project'))
    print(postgres_connect.get_filtered_data_from_selected_table(table_name='Project', Project_ID='python_test_project_2'))
    print(postgres_connect.get_filtered_data_from_selected_table(table_name='Project', ProjectName='python_test_name_2'))
