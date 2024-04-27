# XML Parser Automation
- If you have any questions please don't hesitate to contact me: mustafaoncu815@gmail.com
- ----
- Your directory must have 'EDITOR' and 'STATE' folders that contains corresponding XML files
- .env file also should be exist and file should has database credentials
- If get connection error during script run, please try again
- ------
- Root names are
```python 
['DeadlineSettings', 'ChaosCloudSettings', 'OutputSettings', 'ProjectSettings', 'JarvisSettings']
```
## Base XML Parser
- This class is a parent class which contains abstract methods for polymorphism. 
- Goal is to parse different type of XML files and call same method in main class.

```python
class BaseXMLParser:
    def __init__(self, new_root, path):
        self.root = new_root
        self.path = path
        self.xml_data = []

    @abstractmethod
    def extract_data_to_df(self, project_id):
        pass

    @abstractmethod
    def handle_additional_data(self, project_id):
        pass

    @abstractmethod
    def extract_root_data_to_df(self, project_id):
        pass

    def extract_all_data_to_df(self, project_id):
        dataframes = {}
        root_dataframes = self.extract_data_to_df(project_id)
        dataframes.update(root_dataframes)
        specific_dataframes = self.handle_additional_data(project_id)
        dataframes.update(specific_dataframes)
        return dataframes
```
- Also, this class has common methods for all types of XML files
## State XML Parser
- This class is a child class of BaseXMLParser
- This class helps us to parse State XML files.
- 'extract_root_data_to_df' method helps us to parse chained root branches ('StateSettings', 'State', 'Zone')
- Each 'StateSettings' has 'State' and each 'State' has 'Zone' xml branch. Our goal is to collect all zones in the
corresponding 'State'
- Convert the dataframes
```python
    def extract_root_data_to_df(self, project_id):

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
```
- Then, we add each branch's dataframes to the state_dataframes dict
```python
    def handle_additional_data(self, project_id):
        self.state_dataframes['StateSettings'], self.state_dataframes['State'], self.state_dataframes[
            'Zone'] = self.extract_root_data_to_df(project_id)
        return self.state_dataframes
```
- 'handle_additional_data' also this method is overridden by abstract method (Base XML Parser class) which helps us to manage same process and
same method in Editor XML Parser.
## Editor XML Parser
- This class is a child class of BaseXMLParser
- This class helps us to parse Editor XML files.
- 'extract_root_data_to_df' method helps us to parse chained root branches ('LinkingRecord', 'BasePass', 'OptionPass')
- Goal is to collect all pass data to RenderPass table. 
- 'process_pass' method is used for this purpose.
```python
    def process_pass(self, pass_element, pass_type, pass_id, parent_id, project_id):
        pass_data = {attr: pass_element.get(attr).replace('\n', ',') for attr in pass_element.attrib}
        fields_to_process = ['Layers', 'FeatureCodes', 'Lighting', 'Zones']
        for field in fields_to_process:
            if field in pass_data:
                items_list = [item for item in pass_data[field].split(',') if item] 
                pass_data[field] = "(" + ', '.join(item for item in items_list) + ")" 
        pass_data['RenderPass_ID'] = str(pass_id)
        pass_data['PassType'] = pass_type
        pass_data['BasePass_ID'] = str(
            parent_id) if pass_type == 'OptionPass' else None
        pass_data['LinkingRecord_ID'] = str(
            parent_id) if pass_type == 'BasePass' else None
        pass_data['RenderedScenes'] = None
        pass_data['Project_ID'] = project_id
        self.records_dicts.append(pass_data)
```
- Convert the dataframe
## Normalizer Utils
- Shared Fields
```python
['FeatureCodes', 'Layers', 'Lighting', 'Zones', 'RenderedScenes', 'Exclude', 'Include']
```
- Goal to normalize shared fields in Editor XML files.
- Each parsed from XML files data's dataframe that comes from renderpass dict checked for unique.
```python
    def extract_shared_fields(self):
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
```
- Then, lookup tables create
```python
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
        elif field == 'RenderedScenes':
            if not items_not_in_map:
                default_item = 'DefaultMaxScenes'
                default_id = str(uuid.uuid4())
                lookup_rows = [self.create_rendered_scenes_lookup(field, default_id, default_item)]
            else:
                lookup_rows = [self.create_rendered_scenes_lookup(field, field_id, item) for item, field_id in
                               zip(items_not_in_map, field_ids)]
        else:
            lookup_rows = [pd.DataFrame({f'{field}_ID': [field_id], f'{field}Names': [item], 'Version': 1, 'User': ''})
                           for item, field_id in zip(items_not_in_map, field_ids)]
        self.accumulated_new_rows[field].extend(lookup_rows)
```
- Update renderpass dataframe with corresponding item's id which gave for each unique item in lookup table
```python
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
                self.render_pass_df.rename(columns={'Include_ID': 'OptionInclude_ID', 'Exclude_ID': 'OptionExclude_ID'},
                                           inplace=True)
            else:
                self.render_pass_df[f'{field}_ID'] = [[] for _ in range(len(self.render_pass_df))]
                self.render_pass_df.rename(columns={'Include_ID': 'OptionInclude_ID', 'Exclude_ID': 'OptionExclude_ID'},
                                           inplace=True)
        self.clean_and_update_render_pass()
```
- Finalizes the shared fields DataFrames by concatenating accumulated rows
```python
    def finalize_shared_fields_dfs(self):
        for field in self.shared_fields:
            valid_dfs = [df for df in self.accumulated_new_rows[field] if isinstance(df, pd.DataFrame)]

            if valid_dfs:
                accumulated_df = pd.concat(valid_dfs, ignore_index=True)
                if field in self.shared_fields_dfs and not self.shared_fields_dfs[field].empty:
                    if field in ['Exclude', 'Include']:
                        self.shared_fields_dfs[f'Option{field}'] = pd.concat(
                            [self.shared_fields_dfs[field], accumulated_df],
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
```
- Call 3 method in a method and get normalized dataframes
```python
    def normalize_data(self):
        self.extract_shared_fields()
        self.update_render_pass_table_with_references()
        self.finalize_shared_fields_dfs()

    def get_normalized_dataframes(self):
        return self.render_pass_df, self.shared_fields_dfs
```
## ORM Architecture
- Object-Relational Mapping helps to get efficient, changeable, dynamic queries
- ORM used sqlalchemy and psycopg2 libraries to connect database and to get query
- All filter methods written by ORM architecture
- Those classes are ran for each XML file also database connection should be open whole time. Therefore, 'sql_engine' 
and 'Base' initialization are declared out of all classes at the beginning of the script.
```python
engine_url = f'postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
sql_engine = create_engine(engine_url, pool_size=10, max_overflow=20, pool_timeout=30)
Session = sessionmaker(bind=sql_engine)


def initializer():
    Base = automap_base()
    Base.prepare(autoload_with=sql_engine, schema='public')
    return Base
```
- Reflective and automap base is used because 'load_to_db' method load all dataframes to database. We only should reflect all tables from 
database.
- To reflect tables, tables must have a primary key
```python
    def load_to_db(self, dfs):
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
                    print("Load to DB Error is:", e)

                with sql_engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        primary_keys = inspector.get_pk_constraint(table_name)
                        pk_columns = primary_keys.get('constrained_columns', [])

                        if not pk_columns and table_name != 'ChaosCloudSettings':
                            print(f"Adding primary key to 'public.{table_name}'")
                            conn.execute(
                                text(f'ALTER TABLE "public"."{table_name}" ADD PRIMARY KEY ("{table_name}_ID");'))
                        if not pk_columns and table_name == 'ChaosCloudSettings':
                            print(f"Adding primary key to 'public.{table_name}'")
                            conn.execute(
                                text(f'ALTER TABLE "public"."{table_name}" ADD PRIMARY KEY ("Project_ID");'))
                        trans.commit()
                    except Exception as e:
                        print("Primary Key Error is:", e)
                        trans.rollback()
            else:
                continue
```

## If you have any questions please don't hesitate to contact me: mustafaoncu815@gmail.com



