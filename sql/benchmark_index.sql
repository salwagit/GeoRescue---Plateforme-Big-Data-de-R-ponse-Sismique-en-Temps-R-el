-- =====================================================================
-- Étude d'indexation : sans index vs GIST vs BRIN sur 500 000 points.
-- Lancer :  psql -d georescue -f sql/benchmark_index.sql
-- Relever les "Execution Time" et les noeuds (Seq Scan / Index Scan).
-- Table séparée : n'affecte pas les alertes de l'application.
-- =====================================================================
\timing off
DROP TABLE IF EXISTS bench_alertes;
CREATE TABLE bench_alertes AS
SELECT g AS id, (3 + random() * 4.5)::REAL AS magnitude,
       ST_SetSRID(ST_MakePoint(-9 + random() * 3, 31.5 + random() * 3), 4326) AS geom
FROM generate_series(1, 500000) g;
ANALYZE bench_alertes;

\echo '--- 1. SANS INDEX : zone de 20 km autour de Casablanca (Seq Scan attendu)'
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM bench_alertes
WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-7.6, 33.55), 4326)::geography, 20000);

\echo '--- 2. AVEC GIST sur geometry (index non utilisé car cast ::geography)'
CREATE INDEX idx_bench_geom ON bench_alertes USING GIST (geom);
ANALYZE bench_alertes;
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM bench_alertes
WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-7.6, 33.55), 4326)::geography, 20000);

\echo '--- 3. INDEX sur geography (index utilisé)'
CREATE INDEX idx_bench_geog ON bench_alertes USING GIST ((geom::geography));
ANALYZE bench_alertes;
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM bench_alertes
WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-7.6, 33.55), 4326)::geography, 20000);

\echo '--- 4. K-NN : opérateur <-> (index) vs ST_Distance + ORDER BY (tri complet)'
EXPLAIN (ANALYZE)
SELECT id FROM bench_alertes ORDER BY geom <-> ST_SetSRID(ST_MakePoint(-7.6, 33.55), 4326) LIMIT 5;
EXPLAIN (ANALYZE)
SELECT id FROM bench_alertes
ORDER BY ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(-7.6, 33.55), 4326)::geography) LIMIT 5;

\echo '--- 5. Taille des index (GIST vs BRIN)'
CREATE INDEX idx_bench_brin ON bench_alertes USING BRIN (geom);
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS taille
FROM pg_stat_user_indexes WHERE relname = 'bench_alertes';

DROP TABLE bench_alertes;
