BEGIN;

DROP TABLE IF EXISTS organizations;
DROP TABLE IF EXISTS repos;

CREATE TABLE IF NOT EXISTS organizations(
    id INTEGER NOT NULL,
    login TEXT NOT NULL,
    CONSTRAINT pk_organizations PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS repos(
    git_url TEXT NOT NULL,
    language TEXT NOT NULL,
    archived BOOLEAN NOT NULL,
    forks_count INT NOT NULL,
    open_issues_count INT NOT NULL,
    watchers_count INT NOT NULL,
    CONSTRAINT pk_repos PRIMARY KEY (git_url)
);

COMMIT;