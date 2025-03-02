import pickle
from collections import Counter, defaultdict

import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm
from datasets import load_dataset

from utils import strip_punc

ds = load_dataset("wikimedia/wikipedia", "20231101.en")['train']

with open('stopwords-en.txt', mode='r') as f:
    stopwords = set([line.strip('\n') for line in f.readlines()])

with open('inverse_frequencies.pkl', mode='rb') as f:
    inverse_frequencies = pickle.load(f)

MIN_LENGTH = 20
NUM_KEYWORDS = 10
def reweight(counts):
    return Counter({k: v * inverse_frequencies[k] for k, v in counts.items()
                    if k not in stopwords})

conn = psycopg2.connect("dbname=postgres user=postgres")
cur = conn.cursor()
for article in tqdm(ds):
    # collect keywords for each chunk in the article
    chunks = [para.split(' ') for para in strip_punc(article['text']).lower().split('\n')]
    counters = [reweight(Counter(chunk)) for chunk in chunks]
    wikichunks_rows = []
    keywords_by_chunk = []
    for i, counter in enumerate(counters):
        if len(chunks[i]) < MIN_LENGTH:
            continue
        keywords_by_chunk.append([k for k, _ in counter.most_common(NUM_KEYWORDS)])
        wikichunks_rows.append((article['id'], i, article['title']))

    # add data to wikichunks table
    wikichunks_query = '''
        INSERT INTO wikichunks
        VALUES %s
        RETURNING id;
    '''
    chunk_ids = execute_values(cur, wikichunks_query, wikichunks_rows, '(DEFAULT, %s, %s, %s)', fetch=True)

    # map keywords to chunks
    keywords_rows = defaultdict(list)
    for (chunk_id,), keywords in zip(chunk_ids, keywords_by_chunk):
        for word in keywords:
            keywords_rows[word].append(chunk_id)
    keywords_query = '''
        INSERT INTO keywords
        VALUES %s
        ON CONFLICT (word_hash)
        DO UPDATE SET chunks = keywords.chunks || EXCLUDED.chunks;
    '''
    execute_values(cur, keywords_query, [(hash(k), k, v) for k, v in keywords_rows.items()])
    conn.commit()
cur.close()
conn.close()