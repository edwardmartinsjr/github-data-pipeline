BEGIN;

DROP TABLE IF EXISTS organizations;

CREATE TABLE IF NOT EXISTS organizations(
    id INTEGER NOT NULL,
    login TEXT NOT NULL,
    CONSTRAINT pk_organizations PRIMARY KEY (id)
);

COMMIT;

-- SELECT count(*) FROM organizations;
-- SELECT id FROM organizations ORDER BY 1 DESC LIMIT 1;