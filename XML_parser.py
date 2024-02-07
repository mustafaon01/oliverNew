from abc import abstractmethod
from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

import pandas as pd
import uuid
import os
import traceback


class Databaser:
    def __init__(self):
        load_dotenv()
        self.hostname = os.getenv('DB_HOST')
        self.database = os.getenv('DB_NAME')
        self.username = os.getenv('DB_USER')
        self.pwd = os.getenv('DB_PWD')
        self.port_id = os.getenv('DB_PORT')

    def sql_engine_creator(self):
        sql_engine = create_engine(
            f'postgresql://{self.username}:{self.pwd}@{self.hostname}:{self.port_id}/{self.database}',
            pool_size=100, max_overflow=50
        )
        return sql_engine

    def initializer(self):
        Base = automap_base()
        Base.prepare(self.sql_engine_creator(), reflect=True)
        return Base

    def session_creator(self):
        Session = sessionmaker(bind=self.sql_engine_creator())
        session = Session()
        return session


class BaseXMLParser(Databaser):
    def __init__(self, new_root, root_names, path):
        super().__init__()
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

    def load_to_db(self, dfs):
        sql_engine = self.sql_engine_creator()
        inspector = inspect(sql_engine)

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
                    print("Load db Error is:", e)

                with sql_engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{table_name}_ID");'))
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

    def extract_root_data_to_df(self, project_id):

        for linkingrecord in self.root.findall('.//linkingrecord'):
            linkingrecord_data = {attr: linkingrecord.get(attr).replace('\n', '') for attr in linkingrecord.attrib}
            linkingrecord_data['ID'] = uuid.uuid4()
            linkingrecord_data['Project_ID'] = project_id
            self.linkinrecords_dicts.append(linkingrecord_data)

            for basepass in linkingrecord.findall('.//BasePass'):
                basepass_data = {attr: basepass.get(attr).replace('\n', '') for attr in basepass.attrib}
                basepass_data['ID'] = uuid.uuid4()
                basepass_data['linkingrecord_id'] = linkingrecord_data['ID']
                basepass_data['Project_ID'] = project_id
                self.basepass_dicts.append(basepass_data)

                for optionpass in basepass.findall('.//OptionPass'):
                    optionpass_data = {attr: optionpass.get(attr).replace('\n', '') for attr in optionpass.attrib}
                    optionpass_data['basepass_id'] = basepass_data['ID']
                    optionpass_data['ID'] = uuid.uuid4()
                    optionpass_data['Project_ID'] = project_id
                    self.optionpass_dicts.append(optionpass_data)

        return pd.DataFrame(self.linkinrecords_dicts), pd.DataFrame(self.basepass_dicts), pd.DataFrame(
            self.optionpass_dicts)

    def extract_passes_data_to_render_pass(self, project_id):

        for linking_record in self.root.findall('.//linkingrecord'):
            linking_record_data = {attr: linking_record.get(attr).replace('\n', '') for attr in linking_record.attrib}
            linking_record_id = uuid.uuid4()
            linking_record_data['ID'] = linking_record_id
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
                if field == 'FeatureCodes':
                    items_list = [item for item in pass_data[field].split(',') if item]
                    pass_data[field] = "(" + ', '.join("'" + item + "'" for item in items_list) + ")"
                else:
                    items_list = pass_data[field].split(',')
                    pass_data[field] = "(" + ', '.join("'" + item + "'" for item in items_list) + ")"
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
                    state_data['Layers'] = "(" + ', '.join("'" + item + "'" for item in item_list) + ")"
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
                    zones_str = "(" + ', '.join("'" + zone_name + "'" for zone_name in state_data[field]) + ")"
                    state_data[field] = zones_str
                self.state_dict.append(state_data)

        return pd.DataFrame(self.states_settings_dict), pd.DataFrame(self.state_dict), pd.DataFrame(self.zone_dict)


class NormalizerUtils(Databaser):
    def __init__(self, render_pass_df):
        super().__init__()
        self.render_pass_df = render_pass_df
        self.shared_fields = ['FeatureCodes', 'Layers', 'Lighting', 'Zones', 'RenderedMaxScenes']
        self.shared_fields_dfs = {field: pd.DataFrame(columns=[f'{field}_ID', f'{field}Names', 'version']) for field in
                                  self.shared_fields if
                                  field in self.shared_fields}
        self.field_id_maps = {field: {} for field in self.shared_fields if field in self.shared_fields_dfs}
        self.accumulated_new_rows = {field: [] for field in self.shared_fields}

    def extract_shared_fields(self):
        for field in self.shared_fields:
            if field in self.render_pass_df.columns:
                if self.render_pass_df[field].apply(lambda x: isinstance(x, list)).any():
                    unique_items = self.render_pass_df[field].apply(
                        lambda x: tuple(x) if isinstance(x, list) else x).unique()
                else:
                    unique_items = self.render_pass_df[field].unique()
                for item in unique_items:
                    item = str(item)
                    if item not in self.field_id_maps[field]:
                        try:
                            exist_id = self.filter_method(field, item)
                            if exist_id is None:
                                field_id = self.make_lookup_tables(field, item)
                            else:
                                field_id = exist_id
                        except Exception as e:
                            field_id = self.make_lookup_tables(field, item)
                        self.field_id_maps[field][item] = field_id

    def make_lookup_tables(self, field, item):
        field_id = str(uuid.uuid4())
        if field == 'Zones':
            self.accumulated_new_rows[field].append(self.create_zones_lookup(field, field_id, item))
        elif field == 'FeatureCodes':
            self.accumulated_new_rows[field].append(self.create_feature_code_lookup(field, field_id, item))
        elif field == 'Lighting':
            self.accumulated_new_rows[field].append(self.create_lighting_lookup(field, field_id, item))
        elif field == 'RenderedMaxScenes':
            self.accumulated_new_rows[field].append(self.create_render_max_scenes_lookup(field, field_id, item))
        elif field == 'Layers':
            self.accumulated_new_rows[field].append(self.create_layers_lookup(field, field_id, item))
        else:
            self.accumulated_new_rows[field].append(
                pd.DataFrame({f'{field}_ID': [field_id], f'{field}Names': [item], 'version': 1}))

        return field_id

    def finalize_shared_fields_dfs(self):
        for field in self.shared_fields:
            valid_dfs = [df for df in self.accumulated_new_rows[field] if df is not None]

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

    def create_zones_lookup(self, field, field_id,item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', 'StateName', 'State_ID', 'ZonesNames', 'MaterialNames',
                         'Assignments', 'Version'])
        state_details = self.state_filter_method(item)

        new_row = pd.DataFrame({
            f'{field}_ID': [field_id],
            'StateName': [state_details['StateName']],
            'State_ID': [state_details['State_ID']],
            'ZonesNames': [state_details['ZoneNames']],
            'MaterialNames': [state_details['MaterialNames']],
            'Assignments': [state_details['Assignments']],
            'Version': 1
        })

        return new_row

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

    def create_render_max_scenes_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=['ID', 'Department', 'MaxScene_ID', 'Version'])
        new_row = pd.DataFrame(
            {'ID': [field_id], 'Department': [None], 'MaxScene_ID': [None], 'Version': 1})
        return new_row

    def create_layers_lookup(self, field, field_id, item):
        if field not in self.shared_fields_dfs[field]:
            self.shared_fields_dfs[field] = pd.DataFrame(
                columns=[f'{field}_ID', 'StateName', 'State_ID', 'LayerNames', 'MaxScene_ID',
                         'Version'])
        layer_details = self.layers_filter_method(item)
        new_row = pd.DataFrame(
            {f'{field}_ID': [field_id], 'StateName': [layer_details['StateName']],
             'State_ID': [layer_details['State_ID']], 'LayersNames': [item],
             'MaxScene_ID': [None], 'Version': 1})
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

    def normalize_data(self):
        self.extract_shared_fields()
        self.update_render_pass_table_with_references()
        self.finalize_shared_fields_dfs()

    def get_normalized_dataframes(self):
        return self.render_pass_df, self.shared_fields_dfs

    def filter_method(self, table_name, u_value):
        Base = self.initializer()
        table_obj = getattr(Base.classes, table_name)
        session = self.session_creator()
        column_to_filter = getattr(table_obj, f'{table_name}Names')
        result = session.query(table_obj).filter(column_to_filter == u_value).first()
        if result:
            ID_column_name = f'{table_name}_ID'
            ID_value = getattr(result, ID_column_name)
            return ID_value
        else:
            return None

    def state_filter_method(self, assignments):
        if pd.isna(assignments) or assignments is None:
            return {
                'StateName': [],
                'State_ID': [],
                'ZoneNames': [],
                'MaterialNames': [],
                'Assignments': []
            }
        session = self.session_creator()
        try:
            Base = self.initializer()
            states_table = getattr(Base.classes, 'State')
            query = session.query(
                states_table.Name,
                states_table.State_ID,
                states_table.ZonesNames,
                states_table.MaterialNames,
                states_table.Assignments
            ).filter(states_table.Assignments == assignments)

            result = query.first()
        finally:
            session.close()

        if result:
            aggregated_results = {
                'StateName': result.Name,
                'State_ID': result.State_ID,
                'ZoneNames': result.ZonesNames,
                'MaterialNames': result.MaterialNames,
                'Assignments': result.Assignments,
            }
            return aggregated_results
        else:
            return {
                'StateName': None,
                'State_ID': None,
                'ZoneNames': None,
                'MaterialNames': None,
                'Assignments': assignments
            }

    def layers_filter_method(self, layer):
        session = self.session_creator()
        try:
            Base = self.initializer()
            states_table = getattr(Base.classes, 'State')
            query = session.query(
                states_table.Name,
                states_table.State_ID,
            ).filter(states_table.Layers == layer)

            results = query.all()
        finally:
            session.close()

        if results:
            aggregated_results = {
                'StateName': [result.Name for result in results],
                'State_ID': [result.State_ID for result in results],
            }
            return aggregated_results
        else:
            return {
                'StateName': None,
                'State_ID': None
            }
