import xml.etree.ElementTree as ET
from XML_parser import *


def process_xml_files(xml_paths, root_names, parser_class):
    for i, path in enumerate(xml_paths, start=1):
            project_id = i
            tree = ET.parse(path)
            root = tree.getroot()
            xml_parser = parser_class(root, root_names)
            dfs = xml_parser.extract_all_data_to_df(project_id)
            xml_parser.load_to_db(dfs)


def get_xml_files_from_directory(directory):
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.xml')]


def main():
    editor_directory = 'EDITORS'
    state_directory = 'STATES'

    root_names_editor = ['ProjectSettings', 'ChaosCloudSettings', 'DeadlineSettings', 'OutputSettings',
                         'JARVISSettings']
    root_names_state = ['ProjectSettings']

    editor_paths = get_xml_files_from_directory(editor_directory)
    state_paths = get_xml_files_from_directory(state_directory)
    process_xml_files(editor_paths, root_names_editor, EditorXMLParser)
    process_xml_files(state_paths, root_names_state, StateXMLParser)


if __name__ == '__main__':
    main()
