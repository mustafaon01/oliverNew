import xml.etree.ElementTree as ET
from XML_parser import *
from datetime import datetime
import time


def process_xml_files(xml_paths, root_names, parser_class):
    print(f"All parsing processes are started: {datetime.now().strftime('%H:%M:%S')}")
    for i, path in enumerate(xml_paths, start=1):
        print(f"PATH {i}->", path)
        project_id = create_project_id(path)
        tree = ET.parse(path)
        root = tree.getroot()
        xml_parser = parser_class(root, root_names, path)
        dfs = xml_parser.extract_all_data_to_df(project_id)
        print("All DFs are uploaded", datetime.now().strftime('%H:%M:%S'))
        if 'EDITOR' in path:
            xml_normalizer = NormalizerUtils(dfs['RenderPass'])
            print("Normalize Start", datetime.now().strftime('%H:%M:%S'))
            xml_normalizer.normalize_data()
            print("Normalize end", datetime.now().strftime('%H:%M:%S'))
            normalized_render_pass_df, normalized_common_fields_df = xml_normalizer.get_normalized_dataframes()
            render_pass_dict = {'RenderPass': normalized_render_pass_df}
            print("RenderPass is uploading..", datetime.now().strftime('%H:%M:%S'))
            xml_parser.load_to_db(render_pass_dict)
            print("RenderPass was uploaded and common fields are uploading", datetime.now().strftime('%H:%M:%S'))
            xml_parser.load_to_db(normalized_common_fields_df)
            print("Common fields are uploaded", datetime.now().strftime('%H:%M:%S'))
            dfs = {key: value for key, value in dfs.items() if key != 'RenderPass'}
        print("Common roots are uploading", datetime.now().strftime('%H:%M:%S'))
        xml_parser.load_to_db(dfs)
        print("Common roots are uploaded", datetime.now().strftime('%H:%M:%S'))


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

    print("If there are any existing tables:")
    # BaseXMLParser.print_exist_tables()
    ''' Delete exist tables from database '''
    # BaseXMLParser.delete_exist_tables()
    state_paths = get_xml_files_from_directory(state_directory)
    editor_paths = get_xml_files_from_directory(editor_directory)
    start_time = time.time()
    process_xml_files(state_paths, root_names_state, StateXMLParser)
    process_xml_files(editor_paths, root_names_editor, EditorXMLParser)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"All parsing processes are done time: {datetime.now().strftime('%H:%M:%S')}.")
    print(f"Total Time: {total_time / 60} min")


if __name__ == '__main__':
    main()
