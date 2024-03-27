INSERT INTO traditional.occupation (occupation_code, name, description)
SELECT
    gs.id AS occupation_code,
    'Occupation ' || gs.id AS name,
    'Description for Occupation ' || gs.id AS description
FROM generate_series(1, 50) AS gs(id);
