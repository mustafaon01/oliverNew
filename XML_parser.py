from abc import abstractmethod
from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime

import pandas as pd
import uuid
import os
import traceback

"""
Load environment variables from the specific .env file.
"""
load_dotenv(override=True, dotenv_path='.env')

"""
Retrieve database connection settings from environment variables.
"""
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_PORT = os.getenv('DB_PORT')

"""
Construct the database engine URL and initialize the SQLAlchemy engine.
"""
engine_url = f'postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
sql_engine = create_engine(engine_url, pool_size=10, max_overflow=20, pool_timeout=30)
Session = sessionmaker(bind=sql_engine)


def initializer():
    """
    Initializes and returns the automap base class for reflecting database tables.
    """
    Base = automap_base()
    Base.prepare(autoload_with=sql_engine, schema='public')
    return Base


class BaseXMLParser:
    """
    An abstract base class for XML parsing that provides methods for extracting data,
    managing database operations, and handling project-specific logic.

    This class is designed to be subclassed with implementations for the abstract methods
    that define how XML data is extracted and processed.
    """

    def __init__(self, new_root, path):
        """
        Initializes the XML parser with a given root and file path.
        
        :param new_root: The root element of the XML document.
        :param path: The file path to the XML document.
        """
        self.root = new_root
        self.path = path
        self.xml_data = []

    @abstractmethod
    def extract_data_to_df(self, tag_name, project_id):
        pass

    @abstractmethod
    def handle_additional_data(self, project_id):
        pass

    @abstractmethod
    def extract_root_data_to_df(self, project_id):
        pass

    def extract_all_data_to_df(self, project_id):
        """
        Extracts all relevant data for a given project ID into a collection of DataFrames.
        
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary of DataFrames containing extracted data.
        """
        dataframes = {}
        root_dataframes = self.extract_data_to_df(project_id)
        dataframes.update(root_dataframes)
        specific_dataframes = self.handle_additional_data(project_id)
        dataframes.update(specific_dataframes)
        return dataframes
    
    def create_project_df(self, project_name):
        """
        Creates a DataFrame for a new project and loads it into the database, if the project does not already exist.
        
        :param project_name: The name of the project to create.
        """
        existing_project_id = self.projects_filter_method(project_name)
        if existing_project_id is None:
            project_id = uuid.uuid4()
            project_df = pd.DataFrame({'Project_ID': [project_id], 'ProjectName': [project_name]})
            project_dict = {'Project': project_df}
            self.load_to_db(project_dict)
        else:
            print(f"Project '{project_name}' already exists with Project_ID {existing_project_id}.")
    
    @staticmethod
    def projects_filter_method(project_name):
        session = Session()
        project_id = None
        Base = initializer()
        meta_data = Base.metadata
        if "public.Project" in meta_data.tables:
            project_table = getattr(Base.classes, 'Project')
            query = session.query(project_table).filter(project_table.ProjectName == project_name)
            result = query.first()
            if result:
                project_id = result.Project_ID
        session.close()
        return project_id
    
    @staticmethod
    def update_project_names_with_deadline_outputs():
        Base = initializer()
        session = Session()

        Project = Base.classes.Project
        DeadlineSettings = Base.classes.DeadlineSettings
        projects_with_deadlines = session.query(Project, DeadlineSettings).join(DeadlineSettings, Project.Project_ID == DeadlineSettings.Project_ID).all()

        for project, deadline in projects_with_deadlines:
            project.ProjectName = deadline.Output

        try:
            session.commit()
        except Exception as e:
            session.rollback()
        finally:
            session.close()




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

    @staticmethod
    def print_exist_tables():
        """
        Prints the names of all existing tables in the database.
        """
        Base = initializer() # Initialize the automap base class
        meta_data = Base.metadata # Retrieve the metadata from the base class for getting table names

        with sql_engine.connect() as conn:
            trans = conn.begin()
            try:
                for table_name in meta_data.tables:
                    print("Table Name:", table_name)
                trans.commit()
            except Exception as e:
                trans.rollback()
    
    @staticmethod
    def delete_state_and_zone_table():
        """
        Deletes the 'State', 'Zone' and 'StateSettings' tables from the database.
        """
        Base = initializer() # Initialize the automap base class
        meta_data = Base.metadata # Retrieve the metadata from the base class for getting table names

        with sql_engine.connect() as conn:
            trans = conn.begin()
            try:
                for table_name in meta_data.tables:
                    if table_name in ['public.State', 'public.Zone', 'public.StateSettings']:
                        print(f'Removing {table_name};')
                        conn.execute(text(f'DROP TABLE IF EXISTS "public"."{table_name[7:]}";'))
                trans.commit()
                print("State and Zone tables have been removed.")
            except Exception as e:
                trans.rollback()
    
    @staticmethod
    def modify_jarvis_settings_table():
        """
        Deletes the 'Description' column from the 'JarvisSettings' table using specific SQL query.
        """
        Base = initializer() # Initialize the automap base class
        meta_data = Base.metadata # Retrieve the metadata from the base class for getting table names

        with sql_engine.connect() as conn:
            trans = conn.begin()
            try:
                if 'public.JarvisSettings' in meta_data.tables:
                    conn.execute(text('DELETE From public."JarvisSettings" where "JarvisSettings_ID" in ( with a as (select "JarvisSettings_ID", row_number() over(partition by "Project_ID","FeatureCode" order by "Type" desc) queue from public."JarvisSettings")select "JarvisSettings_ID" from a where queue <> 1)'))
                    trans.commit()
            except Exception as e:
                trans.rollback()

    def load_to_db(self, dfs):
        """
        Loads a collection of DataFrames into the database, creating tables if they do not already exist.
        """
        inspector = inspect(sql_engine) # Retrieve the inspector object for inspecting the database

        for table_name, df in dfs.items():
            if not df.empty:
                try:
                    tables_in_db = inspector.get_table_names()
                    if table_name not in tables_in_db:
                        df.to_sql(table_name, sql_engine, index=False)

                    else:
                        db_columns = inspector.get_columns(table_name)
                        db_column_names = [col['name'] for col in db_columns]
                        df = df.reindex(columns=db_column_names, fill_value=None)
                        df.to_sql(table_name, sql_engine, if_exists='append', index=False)

                except Exception as e:
                    print("Load to DB Error is:", e)

                with sql_engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        primary_keys = inspector.get_pk_constraint(table_name)
                        pk_columns = primary_keys.get('constrained_columns', [])

                        if not pk_columns and table_name != 'ChaosCloudSettings':
                            print(f"Adding primary key to 'public'.'{table_name}'")
                            conn.execute(
                                text(f'ALTER TABLE "public"."{table_name}" ADD PRIMARY KEY ("{table_name}_ID");'))
                        trans.commit()
                    except Exception as e:
                        print("Primary Key Error is:", e)
                        trans.rollback()
            else:
                continue
    

class EditorXMLParser(BaseXMLParser):
    """
    EditorXMLParser is a subclass of BaseXMLParser that provides methods for extracting 'Editor XML' data
    """
    def __init__(self, new_root, path):
        """
        Initializes the EditorXMLParser with a given root and file path.
        :param new_root: The root element of the XML document.
        :param path: The file path to the XML document.
        """
        super().__init__(new_root, path)
        self.editor_dataframes = {}
        self.linkinrecords_dicts = []
        self.basepass_dicts = []
        self.optionpass_dicts = []
        self.records_dicts = []

    def extract_data_to_df(self, project_id):
        """
        Extracts data from the XML document to DataFrames for the 'Editor' specific subcategories.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary of DataFrames containing extracted data.
        """
        root_names = ['DeadlineSettings', 'ChaosCloudSettings', 'OutputSettings', 'ProjectSettings']
        root_dataframes = {}
        for root_name in root_names:
            root_dataframes.update(self.create_root_df(project_id, root_name))
        
        root_dataframes.update(self.create_jarvis_settings_table(project_id))

        return root_dataframes
    
    def create_root_df(self, project_id, root_name):
        """
        Creates a DataFrame for the root element of the XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :param root_name: The name of the root element.
        :return: A dictionary containing the DataFrame for the root element.
        """
        root_df = pd.DataFrame()
        for record in self.root.findall(f'.//{root_name}'):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data[f'{root_name}_ID'] = uuid.uuid4() if root_name != 'ChaosCloudSettings' else None
            record_data['Project_ID'] = project_id
            if root_name == 'ProjectSettings':
                record_data['Type'] = 'Editor'
            current_df = pd.DataFrame([record_data])
            root_df = pd.concat([root_df, current_df], ignore_index=True)

        return {root_name: root_df}

    
    def to_get_descriptions_data_as_dict(self):
        """ 
        Extracts the 'FeatureCode' and 'Description' data from the XML document as a dictionary.
        :return: A dictionary of 'FeatureCode' and 'Description' data.
        """
        descriptions_dict = {desc.get('FeatureCode'): desc.get('Description') for desc in self.root.findall('.//Descriptions/Description')}
        return descriptions_dict
    
    def create_jarvis_settings_table(self, project_id):
        """
        Creates a DataFrame for the 'JarvisSettings' table from the XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary containing the 'JarvisSettings' DataFrame.
        """
        descriptions_dict = self.to_get_descriptions_data_as_dict()
        data = []
        for category in ['Paints', 'Trims', 'Extras', 'Descriptions']:
            for item in self.root.findall(f'.//{category}/{category[:-1]}'):
                feature_code = item.get('FeatureCode')
                description = descriptions_dict.get(feature_code, '')
                data.append({
                    'JarvisSettings_ID': uuid.uuid4(),
                    'Project_ID': project_id,
                    'FeatureCode': feature_code,
                    'Type': category[:-1],
                    'Description': description
                })

        jarvis_settigs_df = pd.DataFrame(data)

        return {'JarvisSettings': jarvis_settigs_df}

    def handle_additional_data(self, project_id):
        """
        Handles additional data extraction for the 'Editor' XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary of DataFrames containing extracted data.
        """
        self.editor_dataframes = self.extract_passes_data_to_render_pass(project_id)
        return self.editor_dataframes

    def extract_passes_data_to_render_pass(self, project_id):
        """
        Extracts data for 'RenderPass' and 'LinkingRecords' from the XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary of DataFrames containing extracted data.
        """
        for linking_record in self.root.findall('.//linkingrecord'):
            linking_record_data = {attr: linking_record.get(attr).replace('\n', '') for attr in linking_record.attrib}
            linking_record_id = uuid.uuid4()
            linking_record_data['LinkingRecords_ID'] = linking_record_id
            linking_record_data['Project_ID'] = project_id
            self.linkinrecords_dicts.append(linking_record_data)
            for base_pass in linking_record.findall('.//BasePass'):
                base_pass_id = uuid.uuid4()
                self.process_pass(base_pass, 'BasePass', base_pass_id, linking_record_id, project_id)
                for option_pass in base_pass.findall('.//OptionPass'):
                    option_pass_id = uuid.uuid4()
                    self.process_pass(option_pass, 'OptionPass', option_pass_id, base_pass_id, project_id)

        render_pass_df = pd.DataFrame(self.records_dicts)
        linking_record_df = pd.DataFrame(self.linkinrecords_dicts)

        return {'RenderPass': render_pass_df,
                'LinkingRecords': linking_record_df}

    def process_pass(self, pass_element, pass_type, pass_id, parent_id, project_id):
        """
        Processes a 'BasePass' or 'OptionPass' element from the XML document.
        :param pass_element: The XML element representing the pass.
        :param pass_type: The type of pass ('BasePass' or 'OptionPass').
        :param pass_id: The ID of the pass.
        :param parent_id: The ID of the parent pass.
        :param project_id: The ID of the project.
        """
        pass_data = {attr: pass_element.get(attr).replace('\n', ',') for attr in pass_element.attrib}
        fields_to_process = ['Layers', 'FeatureCodes', 'Lighting', 'Zones']
        for field in fields_to_process:
            if field in pass_data:
                items_list = [item for item in pass_data[field].split(',') if item] # Split string by comma
                pass_data[field] = "(" + ', '.join(item for item in items_list) + ")" # Convert list to string
        pass_data['RenderPass_ID'] = str(pass_id)
        pass_data['PassType'] = pass_type
        pass_data['BasePass_ID'] = str(parent_id) if pass_type == 'OptionPass' else None # Set BasePass_ID if OptionPass
        pass_data['LinkingRecord_ID'] = str(parent_id) if pass_type == 'BasePass' else None # Set LinkingRecord_ID if BasePass
        pass_data['RenderedScenes'] = None
        pass_data['Project_ID'] = project_id
        ''' State_ID is added to pass_data ''' 
        # TODO: If this line in comment, it takes a lot of time to upload data to database.
        # pass_data['State_ID'] = self.state_filter_method(pass_data['State']) if 'State' in pass_data and pass_data['State'] else None
        self.records_dicts.append(pass_data) # Append pass data to records_dicts list
    
    @staticmethod
    def state_filter_method(state_name):
        """
        Filters the 'State' table by name and returns the corresponding State_ID.
        :param state_name: The name of the state to filter.
        :return: The ID of the state if found, otherwise None."""
        session = Session()
        state_id = None
        Base = initializer()
        meta_data = Base.metadata
        if "public.State" in meta_data.tables:
            project_table = getattr(Base.classes, 'State')
            query = session.query(project_table).filter(project_table.Name == state_name)
            result = query.first()
            if result:
                state_id = result.State_ID
        session.close()
        return state_id


class StateXMLParser(BaseXMLParser):
    """
    StateXMLParser is a subclass of BaseXMLParser that provides methods for extracting 'State XML' data
    """
    def __init__(self, new_root, path):
        """
        Initializes the StateXMLParser with a given root and file path.
        :param new_root: The root element of the XML document.
        :param path: The file path to the XML document."""
        super().__init__(new_root, path)
        self.state_dataframes = {}
        self.states_settings_dict = []
        self.state_dict = []
        self.zone_dict = []

    def extract_data_to_df(self, project_id):
        """
        Extracts data from the XML document to DataFrames for the 'State' specific subcategories(StateSettings).
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary of DataFrames containing extracted data.
        """
        root_dataframes = {}
        root_dataframes.update(self.crate_project_settings_table(project_id))
        return root_dataframes

    def crate_project_settings_table(self, project_id):
        """
        Creates a DataFrame for the 'ProjectSettings' table from the XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary containing the 'ProjectSettings' DataFrame.
        """
        for record in self.root.findall('.//ProjectSettings'):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data['ProjectSettings_ID'] = uuid.uuid4()
            record_data['Project_ID'] = project_id
            record_data['Type'] = 'State'
            state_project_settings_df = pd.DataFrame([record_data])

        return {'ProjectSettings': state_project_settings_df}

    def handle_additional_data(self, project_id):
        """
        Handles additional data(State and StateSettings) extraction for the 'State' XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A dictionary of DataFrames containing extracted data.
        """
        self.state_dataframes['StateSettings'], self.state_dataframes['State'], self.state_dataframes[
            'Zone'] = self.extract_root_data_to_df(project_id)
        return self.state_dataframes

    def extract_root_data_to_df(self, project_id):
        """
        Extracts data for 'StateSettings', 'State' and 'Zone' from the XML document.
        :param project_id: The ID of the project for which data is being extracted.
        :return: A tuple of DataFrames containing extracted data.
        """

        for state_setting in self.root.findall('.//StatesSettings'):
            state_setting_data = {attr: state_setting.get(attr).replace('\n', '') for attr in state_setting.attrib}
            state_setting_data['StateSettings_ID'] = uuid.uuid4()
            state_setting_data['Project_ID'] = project_id
            self.states_settings_dict.append(state_setting_data)

            for state in state_setting.findall('.//State'):
                state_data = {attr: state.get(attr).replace('\n', '') for attr in state.attrib}
                if 'Layers' in state_data:
                    layers_data = [item for item in state_data['Layers'].split(',') if item]
                    layers_str = ", ".join(layers_data)
                    state_data['Layers'] = f"({layers_str})"
                state_data['State_ID'] = uuid.uuid4()
                state_data['StateSettings_ID'] = state_setting_data['StateSettings_ID']
                state_data['Project_id'] = project_id
                state_data['ZonesNames'] = []
                state_data['MaterialNames'] = []
                state_data['Assignments'] = []

                for zone in state.findall('.//Zone'):
                    zone_data = {attr: zone.get(attr).replace('\n', '') for attr in zone.attrib}
                    zone_data['State_ID'] = state_data['State_ID']
                    zone_data['Zone_ID'] = uuid.uuid4()
                    zone_data['Project_ID'] = project_id
                    state_data['Assignments'].append(zone_data['Name'])
                    state_data['MaterialNames'].append(zone_data['Material'])
                    state_data['ZonesNames'].append(zone_data['Zone'])
                    self.zone_dict.append(zone_data)

                fields = ['Assignments', 'MaterialNames', 'ZonesNames']
                for field in fields:
                    zones_str = "(" + ', '.join(zone_name for zone_name in state_data[field]) + ")"
                    state_data[field] = zones_str
                self.state_dict.append(state_data)

        return pd.DataFrame(self.states_settings_dict), pd.DataFrame(self.state_dict), pd.DataFrame(self.zone_dict)


class NormalizerUtils:
    """
    A utility class for normalizing data extracted from XML documents.
    """
    def __init__(self, render_pass_df):
        """
        Initializes the NormalizerUtils with a DataFrame containing extracted render pass data.
        :param render_pass_df: The DataFrame containing render pass data to normalize.
        """
        self.render_pass_df = render_pass_df
        self.shared_fields = ['FeatureCodes', 'Layers', 'Lighting', 'Zones', 'RenderedScenes', 'Exclude', 'Include']
        self.shared_fields_dfs = {field: pd.DataFrame(columns=[f'{field}_ID', f'{field}Names', 'version']) for field in
                                  self.shared_fields}
        self.field_id_maps = {field: {} for field in self.shared_fields}
        self.accumulated_new_rows = {field: [] for field in self.shared_fields} 

    def extract_shared_fields(self):
        """
        Extracts shared fields from the render pass data and creates lookup tables.
        """
        for field in self.shared_fields:
            if field in self.render_pass_df.columns:
                unique_items = self.render_pass_df[field].dropna().unique().tolist()
                value_id_map = self.filter_method(field, unique_items)
                items_not_in_map = [item for item in unique_items if
                                    str(item) not in value_id_map and str(item) not in self.field_id_maps[field]]
                field_ids = [str(uuid.uuid4()) for _ in items_not_in_map]
                self.make_lookup_tables(field, items_not_in_map, field_ids)

                for item in unique_items:
                    if str(item) in value_id_map:
                        self.field_id_maps[field][str(item)] = value_id_map[str(item)]
                    elif str(item) in items_not_in_map:
                        index = items_not_in_map.index(str(item))
                        self.field_id_maps[field][str(item)] = field_ids[index]

    def make_lookup_tables(self, field, items_not_in_map, field_ids):
        """
        Creates lookup tables for shared fields that are not already in the database.
        :param field: The name of the field to create a lookup table for.
        :param items_not_in_map: A list of items not already in the database.
        :param field_ids: A list of IDs for the items not already in the database.
        """
        if field == 'Zones':
            lookup_rows = self.create_zones_lookup(field, items_not_in_map, field_ids)
        elif field == 'Layers':
            lookup_rows = self.create_layers_lookup(field, items_not_in_map, field_ids)
        elif field == 'FeatureCodes':
            lookup_rows = [self.create_feature_code_lookup(field, field_id, item) for item, field_id in
                           zip(items_not_in_map, field_ids)]
        elif field == 'Lighting':
            lookup_rows = [self.create_lighting_lookup(field, field_id, item) for item, field_id in
                           zip(items_not_in_map, field_ids)]
        elif field == 'Exclude':
            lookup_rows = [self.create_exclude_lookup(field, field_id, item) for item, field_id in
                           zip(items_not_in_map, field_ids)]
        elif field == 'Include':
            lookup_rows = [self.create_include_lookup(field, field_id, item) for item, field_id in
                           zip(items_not_in_map, field_ids)]
        elif field == 'RenderedScenes':
            if not items_not_in_map:
                default_item = 'DefaultMaxScenes'
                default_id = str(uuid.uuid4())
                lookup_rows = [self.create_rendered_scenes_lookup(field, default_id, default_item)]
            else:
                lookup_rows = [self.create_rendered_scenes_lookup(field, field_id, item) for item, field_id in
                               zip(items_not_in_map, field_ids)]
        else:
            lookup_rows = [pd.DataFrame({f'{field}_ID': [field_id], f'{field}Names': [item], 'version': 1})
                           for item, field_id in zip(items_not_in_map, field_ids)]
        self.accumulated_new_rows[field].extend(lookup_rows)

    def create_zones_lookup(self, field, items, field_ids):
        """
        Creates a lookup table for 'Zones' based on the extracted data.
        :param field: The name of the field to create a lookup table for.
        :param items: A list of items to create lookup rows for.
        :param field_ids: A list of IDs for the items.
        :return: A list of DataFrames containing lookup rows.
        """
        zone_details = self.state_filter_method(items)
        lookup_rows = []
        for item, field_id in zip(items, field_ids):
            if item in zone_details:
                details = zone_details[item]
                new_row = pd.DataFrame({
                    f'{field}_ID': [field_id],
                    'StateName': [details['StateName']],
                    'State_ID': [details['State_ID']],
                    'ZonesNames': [details['ZoneNames']],
                    'MaterialNames': [details['MaterialNames']],
                    'Assignments': [details['Assignments']],
                    'Version': [1]
                })
                lookup_rows.append(new_row)
        return lookup_rows

    def create_layers_lookup(self, field, items, field_ids):
        """
        Creates a lookup table for 'Layers' based on the extracted data.
        :param field: The name of the field to create a lookup table for.
        :param items: A list of items to create lookup rows for.
        :param field_ids: A list of IDs for the items.
        :return: A list of DataFrames containing lookup rows.
        """
        layer_details = self.layers_filter_method(items)
        lookup_rows = []
        for item, field_id in zip(items, field_ids):
            if item in layer_details:
                state_info = layer_details[item]
                new_row = pd.DataFrame({
                    f'{field}_ID': [field_id],
                    'StateName': state_info['StateName'],
                    'State_ID': state_info['State_ID'],
                    'LayersNames': [item],
                    'Scene_ID': [None],
                    'Version': [1]
                })
                lookup_rows.append(new_row)
        return lookup_rows

    def create_feature_code_lookup(self, field, field_id, item):
        """
        Creates a lookup table for 'FeatureCodes' based on the extracted data.
        :param field: The name of the field to create a lookup table for.
        :param field_id: The ID of the field to create a lookup row for.
        :param item: The item to create a lookup row for.
        :return: A DataFrame containing the lookup row.
        """
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', f'{field}Names', 'JarvisFeed_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], f'{field}Names': [item], 'JarvisFeed_ID': 1, 'Version': 1})
        return new_row

    def create_lighting_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', f'{field}Names', 'Scene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], f'{field}Names': [item], 'Scene_ID': [None], 'Version': 1})
        return new_row

    def create_exclude_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'Option{field}_ID', f'{field}Name', 'Scene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'Option{field}_ID': [field_id], f'{field}Name': [item], 'Scene_ID': [None], 'Version': 1})
        return new_row

    def create_include_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'Option{field}_ID', f'{field}Name', 'Scene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'Option{field}_ID': [field_id], f'{field}Name': [item], 'Scene_ID': [None], 'Version': 1})
        return new_row

    def create_rendered_scenes_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', 'Department', 'Scene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], 'Department': [item], 'Scene_ID': [None], 'Version': 1})
        return new_row

    def update_render_pass_table_with_references(self):
        """
        Updates the render pass table with references to shared fields.
        """
        for field in self.shared_fields:
            if field in self.render_pass_df.columns:
                def map_value_or_list(value):
                    if isinstance(value, (list, tuple)):
                        return [self.field_id_maps[field].get(str(item), item) for item in value]
                    else:
                        return self.field_id_maps[field].get(str(value), value)

                mapped_series = self.render_pass_df[field].apply(map_value_or_list)
                self.render_pass_df[f'{field}_ID'] = mapped_series
                self.render_pass_df.drop(field, axis=1, inplace=True)
            else:
                self.render_pass_df[f'{field}_ID'] = [[] for _ in range(len(self.render_pass_df))]
        self.clean_and_update_render_pass()

    def clean_and_update_render_pass(self):
        """
        Cleans and updates the render pass table by removing unnecessary columns and renaming others.
        """
        columns = ['LightingState', 'OverrideFilename']
        for item in columns:
            if item in self.render_pass_df.columns:
                self.render_pass_df.drop(item, axis=1, inplace=True)
                
        ''' Change the column name from State to BaseState in the RenderPass Table '''
        self.render_pass_df.rename(columns={'State': 'BaseState'}, inplace=True)
        self.render_pass_df['FeatureCodeCurrent_ID'] = None
        self.render_pass_df['LayerCurrent_ID'] = None
        self.render_pass_df['LightingCurrent_ID'] = None
        self.render_pass_df['ZoneCurrent_ID'] = None
        self.render_pass_df['RenderedScenesCurrent_ID'] = None
        self.render_pass_df['ExcludeCurrent_ID'] = None
        self.render_pass_df['IncludeCurrent_ID'] = None

    def finalize_shared_fields_dfs(self):
        """
        Finalizes the shared fields DataFrames by concatenating accumulated rows.
        """
        for field in self.shared_fields:
            valid_dfs = [df for df in self.accumulated_new_rows[field] if isinstance(df, pd.DataFrame)]

            if valid_dfs:
                accumulated_df = pd.concat(valid_dfs, ignore_index=True)
                if field in self.shared_fields_dfs and not self.shared_fields_dfs[field].empty:
                    if field in ['Exclude', 'Include']:
                        self.shared_fields_dfs[f'Option{field}'] = pd.concat([self.shared_fields_dfs[field], accumulated_df],
                                                              ignore_index=True)
                    else:
                        self.shared_fields_dfs[field] = pd.concat([self.shared_fields_dfs[field], accumulated_df],
                                                              ignore_index=True)
                else:
                    if field in ['Exclude', 'Include']:
                        self.shared_fields_dfs[f'Option{field}'] = accumulated_df
                    else:
                        self.shared_fields_dfs[field] = accumulated_df
            else:
                if field not in self.shared_fields_dfs or self.shared_fields_dfs[field].empty:
                    if field in ['Exclude', 'Include']:
                        self.shared_fields_dfs[f'Option{field}'] = pd.DataFrame()    
                    self.shared_fields_dfs[field] = pd.DataFrame()

    def normalize_data(self):
        """
        Normalizes the extracted data by creating lookup tables and updating the render pass table.
        """
        self.extract_shared_fields()
        self.update_render_pass_table_with_references()
        self.finalize_shared_fields_dfs()

    def get_normalized_dataframes(self):
        """
        Returns the normalized render pass DataFrame and shared fields DataFrames.
        """
        return self.render_pass_df, self.shared_fields_dfs

    @staticmethod
    def filter_method(table_name, unique_values):
        """
        Filters a table by a given column and returns a dictionary mapping values to IDs.
        :param table_name: The name of the table to filter.
        :param unique_values: A list of unique values to filter by.
        :return: A dictionary mapping values to IDs.
        """
        Base = initializer()
        meta_data = Base.metadata
        table_name = f'public.{table_name}'
        try:
            if table_name in meta_data.tables:
                table_obj = getattr(Base.classes, table_name[7:])
                session = Session()
                if table_name == 'public.RenderedScenes':
                    filter_column = 'Department'
                elif table_name in ['public.OptionExclude', 'public.OptionInclude']:
                    filter_column = f'{table_name[7:]}Name'
                else:
                    filter_column = f'{table_name[7:]}Names'
                column_to_filter = getattr(table_obj, filter_column)
                ID_column_name = f'{table_name[7:]}_ID'

                results = session.query(table_obj).filter(column_to_filter.in_(unique_values)).all()
                session.close()

                value_id_map = {getattr(result, filter_column): getattr(result, ID_column_name) for result in
                                results}
                return value_id_map
            else:
                return {}
        except Exception as e:
            print(f"Exception occurred: {e}")
            # traceback.print_exc()
            return {}
    
    @staticmethod
    def state_filter_method(assignments_list):
        session = Session()
        state_details_dict = {}
        try:
            Base = initializer()
            states_table = getattr(Base.classes, 'State')
            query = session.query(
                states_table.Name,
                states_table.State_ID,
                states_table.ZonesNames,
                states_table.MaterialNames,
                states_table.Assignments
            ).filter(states_table.Assignments.in_(assignments_list))

            results = query.all()
            for result in results:
                state_details_dict[result.Assignments] = {
                    'StateName': result.Name,
                    'State_ID': result.State_ID,
                    'ZoneNames': result.ZonesNames,
                    'MaterialNames': result.MaterialNames,
                    'Assignments': result.Assignments,
                }
        finally:
            session.close()

        return state_details_dict

    @staticmethod
    def layers_filter_method(layers_list):
        session = Session()
        layers_details_dict = {}
        try:
            Base = initializer()
            states_table = getattr(Base.classes, 'State')
            query = session.query(
                states_table.Name,
                states_table.State_ID,
                states_table.Layers
            ).filter(states_table.Layers.in_(layers_list))

            results = query.all()
            for result in results:
                if result.Name not in layers_details_dict:
                    layers_details_dict[result.Layers] = {
                        'StateName': [],
                        'State_ID': [],
                    }
                layers_details_dict[result.Layers]['StateName'].append(result.Name)
                layers_details_dict[result.Layers]['State_ID'].append(result.State_ID)
        finally:
            session.close()

        return layers_details_dict
