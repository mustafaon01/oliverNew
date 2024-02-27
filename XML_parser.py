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


class BaseXMLParser:
    def __init__(self, new_root, root_names, path):
        self.root = new_root
        self.root_names = root_names
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
        dataframes = {}
        for tag in self.root_names:
            dataframes[tag] = self.extract_data_to_df(tag, project_id)

        specific_dataframes = self.handle_additional_data(project_id)
        dataframes.update(specific_dataframes)
        return dataframes
    
    def create_project_df(self,project_name):
        project_df = pd.DataFrame(columns=['Project_ID', 'ProjectName'])
        project_df['Project_ID'] = uuid.uuid4()
        project_df['ProjectName'] = project_name
        project_dict = {'Project': project_df}
        print("Project df", project_df)
        print(project_dict.items())
        self.load_to_db(project_dict)
    
    @staticmethod
    def projects_filter_method(project_name):
        session = Session()
        project_id = None
        try:
            Base = initializer()
            project_table = getattr(Base.classes, 'Project')
            query = session.query(project_table).filter(project_table.ProjectName == project_name)
            result = query.first()
            if result:
                project_id = result.Project_ID
        except Exception as e:
            print(f"Exception occurred in projects_filter_method: {e}")
            traceback.print_exc()
        finally:
            session.close()
        return project_id
    
    @staticmethod
    def delete_exist_tables():
        Base = initializer()
        meta_data = Base.metadata

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
        Base = initializer()
        meta_data = Base.metadata

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
        Base = initializer()
        meta_data = Base.metadata

        with sql_engine.connect() as conn:
            trans = conn.begin()
            try:
                for table_name in meta_data.tables:
                    if table_name in ['public.State', 'public.Zone']:
                        print(f'Removing {table_name};')
                        conn.execute(text(f'DROP TABLE IF EXISTS "public"."{table_name[7:]}";'))
                trans.commit()
                print("State and Zone tables have been removed.")
            except Exception as e:
                trans.rollback()

    def load_to_db(self, dfs):
        inspector = inspect(sql_engine)

        for table_name, df in dfs.items():
            if table_name == 'Project':
                print("Project is uploading..", datetime.now().strftime('%H:%M:%S'))
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

                        if not pk_columns and table_name not in self.root_names:
                            print(f"Adding primary key to 'public'.'{table_name}'")
                            conn.execute(
                                text(f'ALTER TABLE "public"."{table_name}" ADD PRIMARY KEY ("{table_name}_ID");'))
                        trans.commit()
                    except Exception as e:
                        trans.rollback()
            else:
                continue
    

class EditorXMLParser(BaseXMLParser):
    def __init__(self, new_root, root_names, path):
        super().__init__(new_root, root_names, path)
        self.editor_dataframes = {}
        self.linkinrecords_dicts = []
        self.basepass_dicts = []
        self.optionpass_dicts = []
        self.records_dicts = []

    def extract_data_to_df(self, tag_name, project_id):
        for record in self.root.findall('.//' + tag_name):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data['Project_ID'] = project_id
            if tag_name == 'ProjectSettings':
                record_data['Type'] = 'Editor'
            self.xml_data.append(record_data)
        return pd.DataFrame(self.xml_data)

    def handle_additional_data(self, project_id):
        self.editor_dataframes = self.extract_passes_data_to_render_pass(project_id)
        return self.editor_dataframes

    def extract_passes_data_to_render_pass(self, project_id):
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
        pass_data = {attr: pass_element.get(attr).replace('\n', ',') for attr in pass_element.attrib}
        fields_to_process = ['Layers', 'FeatureCodes', 'Lighting', 'Zones']
        for field in fields_to_process:
            if field in pass_data:
                items_list = [item for item in pass_data[field].split(',') if item]
                pass_data[field] = "(" + ', '.join(item for item in items_list) + ")"
        pass_data['RenderPass_ID'] = str(pass_id)
        pass_data['PassType'] = pass_type
        pass_data['BasePass_ID'] = str(parent_id) if pass_type == 'OptionPass' else None
        pass_data['LinkingRecord_ID'] = str(parent_id) if pass_type == 'BasePass' else None
        pass_data['RenderedMaxScenes'] = None
        pass_data['Project_ID'] = project_id
        self.records_dicts.append(pass_data)

    def find_render_max_id(self):
        max_scenes_ids = []
        if 'pdm' in self.path:
            max_scenes_ids.append('pdm')
        if 'bnp' in self.path:
            max_scenes_ids.append('bnp')
        if 'foe' in self.path:
            max_scenes_ids.append('foe')

        return max_scenes_ids


class StateXMLParser(BaseXMLParser):
    def __init__(self, new_root, root_names, path):
        super().__init__(new_root, root_names, path)
        self.state_dataframes = {}
        self.states_settings_dict = []
        self.state_dict = []
        self.zone_dict = []

    def extract_data_to_df(self, tag_name, project_id):
        for record in self.root.findall('.//' + tag_name):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data['Project_ID'] = project_id
            if tag_name == 'ProjectSettings':
                record_data['Type'] = 'State'
            self.xml_data.append(record_data)
        return pd.DataFrame(self.xml_data)

    def handle_additional_data(self, project_id):
        self.state_dataframes['StateSettings'], self.state_dataframes['State'], self.state_dataframes[
            'Zone'] = self.extract_root_data_to_df(project_id)
        return self.state_dataframes

    def extract_root_data_to_df(self, project_id):

        for state_setting in self.root.findall('.//StatesSettings'):
            state_setting_data = {attr: state_setting.get(attr).replace('\n', '') for attr in state_setting.attrib}
            state_setting_data['StateSettings_ID'] = uuid.uuid4()
            state_setting_data['Project_ID'] = project_id
            self.states_settings_dict.append(state_setting_data)

            for state in state_setting.findall('.//State'):
                state_data = {attr: state.get(attr).replace('\n', '') for attr in state.attrib}
                if 'Layers' in state_data:
                    item_list = state_data['Layers'].split(',')
                    state_data['Layers'] = "(" + ', '.join(item for item in item_list) + ")"
                    # state_data['Layers'] = item_list
                state_data['State_ID'] = uuid.uuid4()
                state_data['StateSettings_ID'] = state_setting_data['StateSettings_ID']
                state_data['Project_id'] = project_id
                state_data['ZonesNames'] = []
                state_data['MaterialNames'] = []
                state_data['Assignments'] = []
                # self.state_dict.append(state_data)

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
    def __init__(self, render_pass_df):
        self.render_pass_df = render_pass_df
        self.shared_fields = ['FeatureCodes', 'Layers', 'Lighting', 'Zones', 'RenderedMaxScenes', 'Exclude', 'Include']
        self.shared_fields_dfs = {field: pd.DataFrame(columns=[f'{field}_ID', f'{field}Names', 'version']) for field in
                                  self.shared_fields}
        self.field_id_maps = {field: {} for field in self.shared_fields}
        self.accumulated_new_rows = {field: [] for field in self.shared_fields}

    def extract_shared_fields(self):
        for field in self.shared_fields:
            print(f"Normalization start for field: {field} time: {datetime.now().strftime('%H:%M:%S')}")
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

            print(f"Normalization end for field: {field} time: {datetime.now().strftime('%H:%M:%S')}")

    def make_lookup_tables(self, field, items_not_in_map, field_ids):
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
        elif field == 'RenderedMaxScenes':
            if not items_not_in_map:
                default_item = 'DefaultMaxScenes'
                default_id = str(uuid.uuid4())
                lookup_rows = [self.create_render_max_scenes_lookup(field, default_id, default_item)]
            else:
                lookup_rows = [self.create_render_max_scenes_lookup(field, field_id, item) for item, field_id in
                               zip(items_not_in_map, field_ids)]
        else:
            lookup_rows = [pd.DataFrame({f'{field}_ID': [field_id], f'{field}Names': [item], 'version': 1})
                           for item, field_id in zip(items_not_in_map, field_ids)]
        self.accumulated_new_rows[field].extend(lookup_rows)

    def create_zones_lookup(self, field, items, field_ids):
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
                    'MaxScene_ID': [None],
                    'Version': [1]
                })
                lookup_rows.append(new_row)
        return lookup_rows

    def create_feature_code_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', f'{field}Names', 'JarvisFeed_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], f'{field}Names': [item], 'JarvisFeed_ID': 1, 'Version': 1})
        return new_row

    def create_lighting_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', f'{field}Names', 'MaxScene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], f'{field}Names': [item], 'MaxScene_ID': [None], 'Version': 1})
        return new_row

    def create_exclude_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', f'{field}Names', 'MaxScene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], f'{field}Names': [item], 'MaxScene_ID': [None], 'Version': 1})
        return new_row

    def create_include_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', f'{field}Names', 'MaxScene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], f'{field}Names': [item], 'MaxScene_ID': [None], 'Version': 1})
        return new_row

    def create_render_max_scenes_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', 'Department', 'MaxScene_ID', 'Version'])
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], 'Department': [item], 'MaxScene_ID': [None], 'Version': 1})
        return new_row

    def update_render_pass_table_with_references(self):
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
        columns = ['LightingState', 'OverrideFilename']
        for item in columns:
            if item in self.render_pass_df.columns:
                self.render_pass_df.drop(item, axis=1, inplace=True)
        self.render_pass_df['FeatureCodeCurrent_ID'] = None
        self.render_pass_df['LayerCurrent_ID'] = None
        self.render_pass_df['LightingCurrent_ID'] = None
        self.render_pass_df['ZoneCurrent_ID'] = None
        self.render_pass_df['RenderedMaxSceneCurrent_ID'] = None
        self.render_pass_df['ExcludeCurrent_ID'] = None
        self.render_pass_df['IncludeCurrent_ID'] = None

    def finalize_shared_fields_dfs(self):
        for field in self.shared_fields:
            valid_dfs = [df for df in self.accumulated_new_rows[field] if isinstance(df, pd.DataFrame)]

            if valid_dfs:
                accumulated_df = pd.concat(valid_dfs, ignore_index=True)
                if field in self.shared_fields_dfs and not self.shared_fields_dfs[field].empty:
                    self.shared_fields_dfs[field] = pd.concat([self.shared_fields_dfs[field], accumulated_df],
                                                              ignore_index=True)
                else:
                    self.shared_fields_dfs[field] = accumulated_df
            else:
                if field not in self.shared_fields_dfs or self.shared_fields_dfs[field].empty:
                    self.shared_fields_dfs[field] = pd.DataFrame()

    def normalize_data(self):
        self.extract_shared_fields()
        self.update_render_pass_table_with_references()
        self.finalize_shared_fields_dfs()

    def get_normalized_dataframes(self):
        return self.render_pass_df, self.shared_fields_dfs

    @staticmethod
    def filter_method(table_name, unique_values):
        Base = initializer()
        meta_data = Base.metadata
        table_name = f'public.{table_name}'
        try:
            if table_name in meta_data.tables:
                table_obj = getattr(Base.classes, table_name[7:])
                session = Session()
                if table_name == 'public.RenderedMaxScenes':
                    filter_column = 'Department'
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
