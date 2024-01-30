from abc import abstractmethod
from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

import pandas as pd
import uuid
import os


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

    def primary_key_maker(self):
        with self.sql_engine_creator().connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text('ALTER TABLE "renderPass" ADD PRIMARY KEY ("RenderPass_ID");'))
                conn.execute(text('ALTER TABLE "FeatureCodes" ADD PRIMARY KEY ("FeatureCodes_ID");'))
                conn.execute(text('ALTER TABLE "AdditionalLayers" ADD PRIMARY KEY ("AdditionalLayers_ID");'))
                conn.execute(text('ALTER TABLE "Depths" ADD PRIMARY KEY ("Depths_ID");'))
                conn.execute(text('ALTER TABLE "Frames" ADD PRIMARY KEY ("Frames_ID");'))
                conn.execute(text('ALTER TABLE "Layers" ADD PRIMARY KEY ("Layers_ID");'))
                conn.execute(text('ALTER TABLE "Lighting" ADD PRIMARY KEY ("Lighting_ID");'))
                conn.execute(text('ALTER TABLE "Zones" ADD PRIMARY KEY ("Zones_ID");'))
                trans.commit()
            except Exception as e:
                trans.rollback()
                print("primary key error:", e)


class BaseXMLParser(Databaser):
    def __init__(self, new_root, root_names):
        super().__init__()
        self.root = new_root
        self.root_names = root_names
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


class EditorXMLParser(BaseXMLParser):
    def __init__(self, new_root, root_names):
        super().__init__(new_root, root_names)
        self.editor_dataframes = {}
        self.linkinrecords_dicts = []
        self.basepass_dicts = []
        self.optionpass_dicts = []
        self.records_dicts = []

    def extract_data_to_df(self, tag_name, project_id):
        for record in self.root.findall('.//' + tag_name):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data['project_id'] = project_id
            if tag_name == 'ProjectSettings':
                record_data['type'] = 'Editor'
            self.xml_data.append(record_data)
        return pd.DataFrame(self.xml_data)

    def handle_additional_data(self, project_id):
        self.editor_dataframes = self.extract_passes_data_to_render_pass(project_id)
        return self.editor_dataframes

    def extract_root_data_to_df(self, project_id):

        for linkingrecord in self.root.findall('.//linkingrecord'):
            linkingrecord_data = {attr: linkingrecord.get(attr).replace('\n', '') for attr in linkingrecord.attrib}
            linkingrecord_data['ID'] = uuid.uuid4()
            linkingrecord_data['project_id'] = project_id
            self.linkinrecords_dicts.append(linkingrecord_data)

            for basepass in linkingrecord.findall('.//BasePass'):
                basepass_data = {attr: basepass.get(attr).replace('\n', '') for attr in basepass.attrib}
                basepass_data['ID'] = uuid.uuid4()
                basepass_data['linkingrecord_id'] = linkingrecord_data['ID']
                basepass_data['project_id'] = project_id
                self.basepass_dicts.append(basepass_data)

                for optionpass in basepass.findall('.//OptionPass'):
                    optionpass_data = {attr: optionpass.get(attr).replace('\n', '') for attr in optionpass.attrib}
                    optionpass_data['basepass_id'] = basepass_data['ID']
                    optionpass_data['ID'] = uuid.uuid4()
                    optionpass_data['project_id'] = project_id
                    self.optionpass_dicts.append(optionpass_data)

        return pd.DataFrame(self.linkinrecords_dicts), pd.DataFrame(self.basepass_dicts), pd.DataFrame(
            self.optionpass_dicts)

    def extract_passes_data_to_render_pass(self, project_id):

        for linking_record in self.root.findall('.//linkingrecord'):
            linking_record_data = {attr: linking_record.get(attr).replace('\n', '') for attr in linking_record.attrib}
            linking_record_id = uuid.uuid4()
            linking_record_data['ID'] = linking_record_id
            linking_record_data['project_id'] = project_id
            self.linkinrecords_dicts.append(linking_record_data)
            for base_pass in linking_record.findall('.//BasePass'):
                base_pass_id = uuid.uuid4()
                self.process_pass(base_pass, 'BasePass', base_pass_id, linking_record_id, project_id)
                for option_pass in base_pass.findall('.//OptionPass'):
                    option_pass_id = uuid.uuid4()
                    self.process_pass(option_pass, 'OptionPass', option_pass_id, base_pass_id, project_id)

        render_pass_df = pd.DataFrame(self.records_dicts)
        linking_record_df = pd.DataFrame(self.linkinrecords_dicts)
        return {'renderPass': render_pass_df,
                'linkingRecords': linking_record_df}

    def process_pass(self, pass_element, pass_type, pass_id, parent_id, project_id):
        pass_data = {attr: pass_element.get(attr, '').replace('\n', '') for attr in pass_element.attrib}
        pass_data['RenderPass_ID'] = str(pass_id)
        pass_data['PassType'] = pass_type
        pass_data['BasePass_ID'] = str(parent_id) if pass_type == 'OptionPass' else None
        pass_data['LinkingRecord_ID'] = str(parent_id) if pass_type == 'BasePass' else None
        pass_data['Project_ID'] = project_id
        self.records_dicts.append(pass_data)


class StateXMLParser(BaseXMLParser):
    def __init__(self, new_root, root_names):
        super().__init__(new_root, root_names)
        self.state_dataframes = {}
        self.states_settings_dict = []
        self.state_dict = []
        self.zone_dict = []

    def extract_data_to_df(self, tag_name, project_id):
        for record in self.root.findall('.//' + tag_name):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data['project_id'] = project_id
            if tag_name == 'ProjectSettings':
                record_data['type'] = 'State'
            self.xml_data.append(record_data)
        return pd.DataFrame(self.xml_data)

    def handle_additional_data(self, project_id):
        self.state_dataframes['statesettings'], self.state_dataframes['states'], self.state_dataframes[
            'zones'] = self.extract_root_data_to_df(project_id)
        return self.state_dataframes

    def extract_root_data_to_df(self, project_id):

        for statesetting in self.root.findall('.//StatesSettings'):
            statesetting_data = {attr: statesetting.get(attr).replace('\n', '') for attr in statesetting.attrib}
            statesetting_data['ID'] = uuid.uuid4()
            statesetting_data['project_id'] = project_id
            self.states_settings_dict.append(statesetting_data)

            for state in statesetting.findall('.//State'):
                state_data = {attr: state.get(attr).replace('\n', '') for attr in state.attrib}
                state_data['ID'] = uuid.uuid4()
                state_data['states_settings_id'] = statesetting_data['ID']
                state_data['project_id'] = project_id
                self.state_dict.append(state_data)

                for zone in state.findall('.//Zone'):
                    zone_data = {attr: zone.get(attr).replace('\n', '') for attr in zone.attrib}
                    zone_data['state_id'] = state_data['ID']
                    zone_data['ID'] = uuid.uuid4()
                    zone_data['project_id'] = project_id
                    self.zone_dict.append(zone_data)

        return pd.DataFrame(self.states_settings_dict), pd.DataFrame(self.state_dict), pd.DataFrame(self.zone_dict)


class NormalizerUtils(Databaser):
    def __init__(self, render_pass_df):
        super().__init__()
        self.render_pass_df = render_pass_df
        self.shared_fields = ['Depths', 'AdditionalLayers', 'FeatureCodes', 'Frames', 'Layers', 'Lighting', 'Zones']
        self.shared_fields_dfs = {field: pd.DataFrame(columns=[f'{field}_ID', 'Desc', 'version']) for field in
                                  self.shared_fields if
                                  field in self.shared_fields}
        self.field_id_maps = {field: {} for field in self.shared_fields if field in self.shared_fields_dfs}

    def extract_shared_fields(self):
        for field in self.shared_fields:
            if field in self.render_pass_df.columns:
                unique_values = self.render_pass_df[field].unique()
                for value in unique_values:
                    value = str(value)
                    if value not in self.field_id_maps[field]:
                        try:
                            exist_id = self.filter_method(field, value)
                            if exist_id is None:
                                field_id = str(uuid.uuid4())
                                new_row = pd.DataFrame(
                                    {f'{field}_ID': [field_id], 'Desc': [value],
                                     'version': '1.0.0'})
                                self.shared_fields_dfs[field] = pd.concat([self.shared_fields_dfs[field], new_row],
                                                                          ignore_index=True)
                            else:
                                field_id = exist_id
                        except Exception as e:
                            field_id = str(uuid.uuid4())
                            if field not in self.shared_fields_dfs or not isinstance(self.shared_fields_dfs[field],
                                                                                     pd.DataFrame):
                                self.shared_fields_dfs[field] = pd.DataFrame(
                                    columns=[f'{field}_ID', 'Desc', 'version'])
                            new_row = pd.DataFrame({f'{field}_ID': [field_id], 'Desc': [value],
                                                    'version': '1.0.0'})
                            self.shared_fields_dfs[field] = pd.concat([self.shared_fields_dfs[field], new_row],
                                                                      ignore_index=True)
                        self.field_id_maps[field][value] = field_id

    def update_render_pass_table_with_references(self):
        for field in self.shared_fields:
            if field in self.render_pass_df.columns:
                self.render_pass_df[field] = self.render_pass_df[field].map(self.field_id_maps[field])
                self.render_pass_df[f'{field}_ID'] = self.render_pass_df[field]
                self.render_pass_df.drop(field, axis=1, inplace=True)

    def normalize_data(self):
        self.extract_shared_fields()
        self.update_render_pass_table_with_references()

    def get_normalized_dataframes(self):
        return self.render_pass_df, self.shared_fields_dfs

    def filter_method(self, table_name, u_value):
        Base = self.initializer()
        table_obj = getattr(Base.classes, table_name)
        session = self.session_creator()
        # column_to_filter = getattr(table_obj, table_name)

        result = session.query(table_obj).filter(table_obj.Desc == u_value).first()
        if result:
            ID_column_name = f'{table_name}_ID'
            ID_value = getattr(result, ID_column_name)
            return ID_value
        else:
            return None
