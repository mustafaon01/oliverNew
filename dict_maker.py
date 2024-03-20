from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

import os

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_PORT = os.getenv('DB_PORT')


engine_url = f'postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
sql_engine = create_engine(engine_url, pool_size=10, max_overflow=20, pool_timeout=30)
Session = sessionmaker(bind=sql_engine)


def initializer():
    Base = automap_base()
    Base.prepare(sql_engine, reflect=True, schema='public')
    return Base


Base = initializer()
class ToDictMixin(object):
    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}


for class_ in Base.classes:
    class_.__bases__ = (ToDictMixin,) + class_.__bases__


def query_to_dict(session, table_name, field_name, field_value):
    """
    Queries a table in the database and convert to dict
    param: session: a session object
    param: table_name: the name of the table to query
    param: field_name: the name of the field to query
    param: field_value: the value of the field to query
    return: a list of dictionaries, each dictionary represents a row in the table
    """
    try:
        table = getattr(Base.classes, table_name)
        results = session.query(table).filter(getattr(table, field_name) == field_value).all()
        return [result.to_dict() for result in results]

    finally:
        session.close()


def insert_data(session, table_name, **kwargs):
    """
    Inserts data into a table.
    param: session: SQLAlchemy session object
    param: table_name: the name of the table to insert data into
    param: data: keyword arguments representing the data to insert
    """
    try:
        table = getattr(Base.classes, table_name)
        new_record = table(**kwargs)
        session.add(new_record)
        session.commit()
        print(f"Data inserted successfully into {table_name}.")
    except Exception as e:
        session.rollback()
        print(f"Error inserting data into {table_name}: {e}")
    finally:
        session.close()


# kwargs which included table_name
def filter_records_with_table_name(session, **kwargs):
    """
    Queries and filters a table based on given keyword arguments including the table name.
    param session: SQLAlchemy session object
    param kwargs: keyword arguments representing the table name and filter conditions
    return: a list of dictionaries, each representing a row in the table
    """
    table_name = kwargs.pop('table_name', None)
    if not table_name:
        print("Table name must be provided with 'table_name' key.")
        return []
    try:
        table = getattr(Base.classes, table_name)
        query = session.query(table)
        for key, value in kwargs.items():
            query = query.filter(getattr(table, key) == value)
        results = query.all()
        return [result.to_dict() for result in results]
    except Exception as e:
        print(f"An error occurred while filtering records: {e}")
        return []
    finally:
        session.close()


def filter_records(session, table_name, **kwargs):
    """
    Queries and filters a table based on given keyword arguments.
    param session: SQLAlchemy session object
    param table_name: the name of the table to query
    param kwargs: keyword arguments representing the filter conditions
    return: a list of dictionaries, each representing a row in the table
    """
    try:
        table = getattr(Base.classes, table_name)
        query = session.query(table)
        for key, value in kwargs.items():
            query = query.filter(getattr(table, key) == value)
        results = query.all()
        return [result.to_dict() for result in results]
    except Exception as e:
        print(f"An error occurred while filtering records: {e}")
        return []
    finally:
        session.close()


if __name__ == '__main__':
    session = Session()
    result_dict = query_to_dict(session, 'Lighting', 'LightingNames', "(0_configurator_cameras, 0_lighting)")
    result_new_filter_method = filter_records(session, 'Lighting', **result_dict[0])
    print(result_dict)
    print(result_new_filter_method)
