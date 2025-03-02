CREATE TABLE wikichunks (
    id SERIAL PRIMARY KEY,
    article_id INT,
    chunk_index INT,
    article_title TEXT
);