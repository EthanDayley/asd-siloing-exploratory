import xml.etree.ElementTree as ET
import os
import mysql.connector
import pandas as pd

if __name__ == '__main__':
    # set up parameters
    NLM_CATALOG_RES_PATH = os.path.join('journal_classification', 'nlm_catalog_results')

    # connect to database
    DB_PARAM_FILEPATH = os.path.join('db_connection_params.csv')
    db_params = pd.read_csv(DB_PARAM_FILEPATH)
    db_host = db_params.host.iloc[0]
    db_user = db_params.username.iloc[0]
    db_pass = db_params.password.iloc[0]
    db_name = db_params.database.iloc[0]
    db_table = db_params.table.iloc[0]

    print('Connecting to database...')
    cnx = mysql.connector.connect(user = db_user, password = db_pass, host = db_host, database = db_name)
    cursor = cnx.cursor(buffered=True)
    print('Done connecting to database.')

    # load list of files in directory
    filenames = os.listdir(NLM_CATALOG_RES_PATH)

    medline_ta_list = []

    for filename in filenames:
        filepath = os.path.join(NLM_CATALOG_RES_PATH, filename)

        # load nlm catalog result
        print()
        print('loading nlm catalog result ({0})'.format(filename))
        tree = ET.ElementTree(file=filepath)
        tree_root = tree.getroot()
        print('done loading nlm catalog result')

        # iterate through sources
        for record in tree_root:
            try:
                ncbi_source_id = str(record.attrib['ID'])
            except:
                print('Failed to extract ncbi_source_id from {0}'.format(record))
                continue
            try:
                nlm_catalog_record = record.find('NLMCatalogRecord')
            except:
                print('Failed to extract nlm catalog record from {0}'.format(record))
                continue
            try:
                medline_ta = nlm_catalog_record.find('MedlineTA').text
                medline_ta_list.append(medline_ta)
            except:
                print('Failed to extract medline_ta from {0}'.format(nlm_catalog_record))
                continue
            try:
                full_source_title = nlm_catalog_record.find('TitleMain').find('Title').text
            except:
                print('Failed to extract full source title from {0}'.format(nlm_catalog_record))
                continue
            try:
                for mesh_heading in nlm_catalog_record.find('MeshHeadingList'):
                    try:
                        descriptor = mesh_heading.find('DescriptorName').text

                        try:
                            # check if record is already downloaded
                            query = 'SELECT ncbi_source_id FROM source_descriptors WHERE medline_ta = "{0}" AND descriptor = "{1}"'.format(medline_ta, descriptor)
                            cursor.execute(query)
                            row_count = cursor.rowcount
                            if row_count != 0:
                                print('Record with medline_ta = "{0}" and descriptor = "{1}" already present, skipping'.format(medline_ta, descriptor))
                                continue

                            query = '''INSERT INTO source_descriptors (ncbi_source_id, medline_ta, descriptor, source_title)
                                       VALUES ("{0}", "{1}", "{2}", "{3}")
                                    '''.format(ncbi_source_id, medline_ta, descriptor, full_source_title)
                            print('Inserting record with medline_ta = "{0}" and descriptor = "{1}"'.format(medline_ta, descriptor))
                            cursor.execute(query)
                            cnx.commit()

                        except Exception as e:
                            print('Unable to INSERT/UPDATE with following query: \n{0}'.format(query))
                            print(e)
                            continue
                    except:
                        continue
            except TypeError as e:
                print('Skipping entry with medline_ta "{0}" from file "{1}"'.format(medline_ta, filepath))
                print('Exception: "{0}"'.format(e))
                continue
        print()
    print('Done')
    print('{0} distinct medline_ta values processed'.format(len(set(medline_ta_list))))