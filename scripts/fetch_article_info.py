from datetime import datetime
import json
import mysql.connector
import os
import pandas as pd
import time
import urllib.request

def fetch_article_info(info_href: str, pmc_id: str) -> tuple:
    # returns time to fetch info in seconds, article info
    t0 = time.time()
    truncated_pmc_id = pmc_id[3:]
    url = urllib.request.urlopen(info_href.format(truncated_pmc_id))
    data = url.read()
    encoding = url.info().get_content_charset('utf-8')
    t1 = time.time()
    return (json.loads(data.decode(encoding)), t1 - t0)


if __name__ == '__main__':
    # set path to PMC ID file
    PMC_ID_PATH = os.path.join('search', 'article-test-subset.txt')

    # set up prefix for notices
    NOTICE_PREFIX = 'NOTICE: '
    DOC_INFO_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&id={0}&retmode=json'

    # set path to file containing database connection parameters
    DB_PARAM_FILEPATH = os.path.join('db_connection_params.csv')

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

    # load PMC ids
    with open(PMC_ID_PATH, 'r') as f:
        pmc_ids = [i.strip() for i in f.read().splitlines()]

    # process all articles by PMC ID
    for pmc_id in pmc_ids:
        print('Processing "{0}"'.format(pmc_id))
        
        print('downloading article info')
        try:
            data, dt = fetch_article_info(DOC_INFO_URL, pmc_id)
        except:
            msg = '{0} Failed to download article info for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            print(msg)
            print()
        print('download complete in {0} seconds.'.format(dt))

        try:
            pub_date = data['result'][pmc_id[3:]]['pubdate']
            pub_date = datetime.strptime(pub_date, '%Y %b %d').date()
        except:
            msg = '{0} Failed to extract pub_date for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            print(msg)
            print()
            pub_date = None
            
        try:
            source = data['result'][pmc_id[3:]]['source']
        except:
            msg = '{0} Failed to extract source for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            print(msg)
            print()
            source = ''
        try:
            article_title = data['result'][pmc_id[3:]]['title']
        except:
            msg = '{0} Failed to extract article title for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            print(msg)
            print()
            article_title = ''

        # update article with new info
        print('updating entry in database')
        try:
            query = 'UPDATE {0} SET pub_date=%(pub_date)s, source=%(source)s, article_title=%(article_title)s WHERE pmc_id=%(pmc_id)s'.format(db_table)
            print(query)
            cursor.execute(query, {'pub_date': pub_date, 'source': source, 'article_title': article_title, 'pmc_id': pmc_id})
            cnx.commit()
        except:
            msg = '{0} Failed to update row into database for PMC ID {1}'.format(NOTICE_PREFIX, pmc_id)
            print(msg)
            print()
            continue
        print('done updating')

        print()