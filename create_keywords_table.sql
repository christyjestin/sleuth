CREATE TABLE keywords (
    word_hash BIGINT PRIMARY KEY,
    word TEXT,
    chunks INT[]
);