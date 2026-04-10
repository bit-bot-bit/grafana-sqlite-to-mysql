TRUNCATE TABLE team;
TRUNCATE TABLE org;

INSERT INTO org (id, name, slug, created_ms) VALUES (1, 'org-1', 'slug-1', 1700000000001);
INSERT INTO org (id, name, slug, created_ms) VALUES (2, 'org-2', 'slug-2', 1700000000002);
INSERT INTO org (id, name, slug, created_ms) VALUES (1, 'org-dup', 'slug-dup', 1700000000999);
INSERT INTO org (id, name, slug, created_ms) VALUES (3, 'org-3', 'slug-3', 1700000000003);

INSERT INTO team (id, org_id, name, email) VALUES (10, 1, 'team-1', 'team1@example.test');
