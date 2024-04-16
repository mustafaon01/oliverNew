```python

@staticmethod
    def delete_exist_tables():
        """
        Deletes all existing tables from the database before starting the script.
        """
        Base = initializer() # Initialize the automap base class
        meta_data = Base.metadata # Retrieve the metadata from the base class for getting table names

        with sql_engine.connect() as conn:
            trans = conn.begin()
            try:
                for table_name in meta_data.tables:
                    print(f'Dropping {table_name};')
                    conn.execute(text(f'DROP TABLE IF EXISTS "public"."{table_name[7:]}";'))
                trans.commit()
                print("Exist tables have been removed before starting the script")
            except Exception as e:
                trans.rollback()
                
```