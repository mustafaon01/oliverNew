from abc import abstractmethod
from sqlalchemy import *
from dotenv import load_dotenv

import pandas as pd
import uuid
import os


class BaseXMLParser:
    def __init__(self, new_root, root_names):
        load_dotenv()
        self.root = new_root
        self.root_names = root_names
        self.hostname = os.getenv('DB_HOST')
        self.database = os.getenv('DB_NAME')
        self.username = os.getenv('DB_USER')
        self.pwd = os.getenv('DB_PWD')
        self.port_id = os.getenv('DB_PORT')
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
        engine = create_engine(
            f'postgresql://{self.username}:{self.pwd}@{self.hostname}:{self.port_id}/{self.database}',
            pool_size=100, max_overflow=50
        )
        inspector = inspect(engine)

        for table_name, df in dfs.items():
            try:
                tables_in_db = inspector.get_table_names()
                if table_name not in tables_in_db:
                    df.to_sql(table_name, engine, index=False)
                else:
                    db_columns = inspector.get_columns(table_name)
                    db_column_names = [col['name'] for col in db_columns]
                    df = df.reindex(columns=db_column_names, fill_value=None)
                    df.to_sql(table_name, engine, if_exists='append', index=False)
            except Exception as e:
                print("Error is:", e)
                print(df)


class EditorXMLParser(BaseXMLParser):
    def __init__(self, new_root, root_names):
        super().__init__(new_root, root_names)
        self.editor_dataframes = {}
        self.linkinrecords_dicts = []
        self.basepass_dicts = []
        self.optionpass_dicts = []

    def extract_data_to_df(self, tag_name, project_id):
        for record in self.root.findall('.//' + tag_name):
            record_data = {attr: record.get(attr).replace('\n', '') for attr in record.attrib}
            record_data['project_id'] = project_id
            if tag_name == 'ProjectSettings':
                record_data['type'] = 'Editor'
            self.xml_data.append(record_data)
        return pd.DataFrame(self.xml_data)

    def handle_additional_data(self, project_id):
        self.editor_dataframes['linkingrecords'], self.editor_dataframes['basepasses'], self.editor_dataframes[
            'optionpasses'] = self.extract_root_data_to_df(project_id)
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
