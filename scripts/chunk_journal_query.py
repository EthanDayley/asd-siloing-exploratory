import os

if __name__ == '__main__':
    QUERY_FILEPATH = os.path.join('journal_classification', 'nlm_catalog_query.txt')
    MAX_LENGTH = 2000

    with open(QUERY_FILEPATH, 'r') as f:
        split_query = f.read().split(' OR ')
    
    idx = 0
    chunk = 1
    while idx < len(split_query):
        s = split_query[idx]
        idx += 1

        while idx < len(split_query):
            if len(s) + len(' OR ') + len(split_query[idx]) < MAX_LENGTH:
                s += ' OR {0}'.format(split_query[idx])
                idx += 1
            else:
                break
        print('[{0}]'.format(chunk))
        print(s)
        print()
        chunk += 1