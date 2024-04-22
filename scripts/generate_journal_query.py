import io
import os
import string

if __name__ == '__main__':
    JOURNAL_LIST_PATH = 'journal_list.txt'
    QUERY_OUTPUT_PATH = os.path.join('journal_classification', 'nlm_catalog_query.txt')

    print('reading journal list from "{0}"'.format(JOURNAL_LIST_PATH))
    with io.open(JOURNAL_LIST_PATH, 'r', encoding='utf-16-le') as f:
        printable = set(string.printable)
        journals = [''.join(filter(lambda x: x in printable, i)) for i in f.read().splitlines()]
    print('done reading journals')
    
    print('generating query terms')
    query_terms = ['"{0}"[NLM Title Abbreviation]'.format(journal.strip()) for journal in journals]
    query = ' OR '.join(query_terms)
    print('done generating query terms')

    print('writing query to "{0}"'.format(QUERY_OUTPUT_PATH))
    with open(QUERY_OUTPUT_PATH, 'w') as f:
        f.write(query)
    print('done')