import urllib.request
import xml.etree.ElementTree as ET
import os
import time
import tarfile
import string
import re
import mysql.connector
import pandas as pd
import shutil

def get_href_by_pmc_id(pmc_id, doc_info_url):
    # fetch from API
    doc_info = urllib.request.urlopen(DOC_INFO_URL.format(pmc_id)).read()
    
    # parse XML
    doc_info_tree = ET.ElementTree(ET.fromstring(doc_info))
    doc_info_root = doc_info_tree.getroot()
    doc_info_record = doc_info_root.find('./records/record')
    for link in doc_info_record.findall('./'):
        if link.attrib['format'] == 'tgz':
            return link.attrib['href']
    raise RuntimeError('Unable to find link to article with PMC ID "{0}"'.format(pmc_id))
    
def fetch_article_archive(article_href, filepath):
    # returns time to fetch archive in seconds
    t0 = time.time()
    urllib.request.urlretrieve(article_href, filepath)
    t1 = time.time()
    return t1 - t0

def unzip_archive(archive_path, output_path):
    # returns time to unzip archive in seconds
    t0 = time.time()
    file = tarfile.open(archive_path)
    file.extractall(output_path)
    file.close()
    t1 = time.time()
    return t1 - t0

def get_cleaned_abstract(full_extracted_path):
    # start by getting the xml filename
    filenames = os.listdir(full_extracted_path)
    nxml_filename = None
    for filename in filenames:
        if '.nxml' in filename:
            nxml_filename = filename
            break

    if not nxml_filename:
        raise RuntimeError('No XML file found for path {0}'.format(full_extracted_path))
    
    # read xml file
    tree = ET.parse(os.path.join(full_extracted_path, nxml_filename))
    root = tree.getroot()
    abstr = root.find('.//abstract')
    abstr_text = ET.tostring(abstr, encoding='utf8', method='text').decode('utf-8')

    # clean abstract text
    abstr_text = abstr_text.lower()
    abstr_text = re.sub(f"[{re.escape(string.punctuation)}]", "", abstr_text)
    return abstr_text

if __name__ == '__main__':
    PMC_ID_PATH = os.path.join('..', 'search', 'article-test-subset.txt')
    OUTPUT_PATH = os.path.join('..', 'download_abstracts')
    TMP_PATH = 'tmp'
    NOTICE_PREFIX = 'NOTICE: '
    LOG_PATH = os.path.join(OUTPUT_PATH, 'log.txt')

    DOC_INFO_URL = 'https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={0}'

    # open log files
    log_file = open(LOG_PATH, 'a')

    # connect to database
    DB_PARAM_FILEPATH = os.path.join('..', 'db_connection_params.csv')
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

    # create tmp path
    if not os.path.exists(os.path.join(OUTPUT_PATH, TMP_PATH)):
        os.makedirs(os.path.join(OUTPUT_PATH, TMP_PATH))

    # load PMC ids
    with open(PMC_ID_PATH, 'r') as f:
        pmc_ids = [i.strip() for i in f.read().splitlines()]

    for pmc_id in pmc_ids:
        print('Processing "{0}"'.format(pmc_id))

        # check if record is already present in database
        query = 'SELECT pmc_id FROM {0} WHERE pmc_id = "{1}"'.format(db_table, pmc_id)
        cursor.execute(query)
        row_count = cursor.rowcount
        if row_count != 0:
            print('{0} PMC_ID: "{1}" already present, skipping'.format(NOTICE_PREFIX, pmc_id))
            print()
            continue

        # fetch document href
        print('Fetching article href')
        try:
            article_href = get_href_by_pmc_id(pmc_id, DOC_INFO_URL)
        except:
            msg = '{0} Failed to fetch article href for PMC Id "{1}"'.format(NOTICE_PREFIX, pmc_id)
            log_file.write(msg+'\n')
            print(msg)
            print()
            continue

        # download via ftp
        archive_path = os.path.join(OUTPUT_PATH, TMP_PATH, '{0}.tar.gz'.format(pmc_id))
        print('downloading {0} to {1}'.format(article_href, archive_path))
        try:
            dt = fetch_article_archive(article_href, archive_path)
        except:
            msg = '{0} Failed to download article archive for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            log_file.write(msg+'\n')
            print(msg)
            print()
            continue
        print('download complete in {0} seconds.'.format(dt))

        # extract archive
        extracted_path = os.path.join(OUTPUT_PATH, TMP_PATH)
        print('extracting archive to {0}'.format(extracted_path))
        try:
            dt = unzip_archive(archive_path, extracted_path)
        except:
            msg = '{0} Failed to extract article archive for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            log_file.write(msg+'\n')
            print(msg)
            print()
            continue
        print('extraction complete in {0} seconds'.format(dt))

        # extract abstract
        print('cleaning abstract')
        try:
            abstr = get_cleaned_abstract(os.path.join(extracted_path, pmc_id))
        except:
            msg = '{0} Failed to extract cleaned abstract for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            log_file.write(msg+'\n')
            print(msg)
            print()
            continue
        print('done cleaning abstract')

        # insert row into database
        print('inserting into database')
        try:
            query = 'INSERT INTO {0} (pmc_id, archive_href, abstract_text) VALUES ("{1}", "{2}", "{3}")'.format(db_table, pmc_id, article_href, abstr)
            cursor.execute(query)
            cnx.commit()
        except:
            msg = '{0} Failed to insert row into database for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            log_file.write(msg+'\n')
            print(msg)
            print()
            continue
        print('done inserting')

        # delete archive and extracted files
        print('removing temporary files')
        os.remove(archive_path)
        shutil.rmtree(os.path.join(extracted_path, pmc_id))
        print('done removing temporary files')
        print()
    log_file.close()
    print('ALL IDs PROCESSED')