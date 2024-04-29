USE asd_siloing;

CREATE TABLE IF NOT EXISTS articles (
    pmc_id VARCHAR(100) NOT NULL,
    archive_href VARCHAR(400),
    abstract_text TEXT,
    pub_date DATE,
    source VARCHAR(300) NOT NULL,
    article_title VARCHAR(400),
    PRIMARY KEY (pmc_id)
);

CREATE TABLE IF NOT EXISTS source_descriptors (
    ncbi_source_id VARCHAR(50),
    medline_ta VARCHAR(400),
    descriptor VARCHAR(400),
    source_title VARCHAR(300)
);