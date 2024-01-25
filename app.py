import xml.etree.ElementTree as ET
from XML_parser import *


def process_xml_files(xml_paths, root_names, parser_class):
    for i, path in enumerate(xml_paths, start=1):
        print(f"{i}.PATH->", path)
        project_id = create_project_id(path)
        tree = ET.parse(path)
        root = tree.getroot()
        xml_parser = parser_class(root, root_names)
        dfs = xml_parser.extract_all_data_to_df(project_id)
        if 'EDITOR' in path:
            xml_normalizer = NormalizerUtils(dfs['renderPass'])
            xml_normalizer.normalize_data()
            normalized_render_pass_df, normalized_common_fields_df = xml_normalizer.get_normalized_dataframes()
            render_pass_dict = {'renderPass': normalized_render_pass_df}
            xml_parser.load_to_db(render_pass_dict)
            xml_parser.load_to_db(normalized_common_fields_df)
            dfs = {key: value for key, value in dfs.items() if key != 'renderPass'}
            if i == 1:
                xml_normalizer.primary_key_maker()
        xml_parser.load_to_db(dfs)


def create_project_id(path):
    index = path.find('/') + 1
    if len(path) > 20:
        index += 7
    project_id = path[index:-4]
    return project_id


# TODO: we can find paths according to included name.
def get_xml_files_from_directory(directory):
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.xml')]


def main():
    editor_directory = 'EDITORS'
    state_directory = 'STATES'

    root_names_editor = ['ProjectSettings', 'ChaosCloudSettings', 'DeadlineSettings', 'OutputSettings',
                         'JARVISSettings']
    root_names_state = ['ProjectSettings']

    state_paths = get_xml_files_from_directory(state_directory)
    editor_paths = get_xml_files_from_directory(editor_directory)
    process_xml_files(state_paths, root_names_state, StateXMLParser)
    process_xml_files(editor_paths, root_names_editor, EditorXMLParser)


if __name__ == '__main__':
    main()
