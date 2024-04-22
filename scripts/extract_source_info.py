import xml.etree.ElementTree as ET
import os
import mysql.connector
import pandas as pd

if __name__ == '__main__':
    # set up parameters
    NLM_CATALOG_RES_PATH = os.path.join('journal_classification', 'nlmcatalog_result.xml')

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

    # load nlm catalog result
    print('loading nlm catalog result')
    tree = ET.ElementTree(file=NLM_CATALOG_RES_PATH)
    tree_root = tree.getroot()
    print('done loading nlm catalog result')

    # iterate through sources
    for record in tree_root:
        ncbi_source_id = str(record.attrib['ID'])
        nlm_catalog_record = record.find('NLMCatalogRecord')
        medline_ta = nlm_catalog_record.find('MedlineTA').text
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
                            continue

                        query = '''INSERT INTO source_descriptors (ncbi_source_id, medline_ta, descriptor)
                                   VALUES ("{0}", "{1}", "{2}")
                                '''.format(ncbi_source_id, medline_ta, descriptor)
                        cursor.execute(query)
                        cnx.commit()

                    except Exception as e:
                        print('Unable to INSERT/UPDATE with following query: \n{0}'.format(query))
                        print(e)
                        continue
                except:
                    continue
        except TypeError:
            continue
    print('Done')