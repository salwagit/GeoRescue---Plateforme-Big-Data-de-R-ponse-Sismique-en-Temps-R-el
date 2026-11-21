-- =====================================================================
-- Requêtes spatiales avancées GeoRescue (à exécuter dans psql / pgAdmin)
-- Chaque requête répond à une question métier.
-- =====================================================================

-- Q1. Jointure spatiale : quels secteurs sont touchés par la dernière alerte,
--     et quelle part de leur surface est dans la zone sévère ?
SELECT s.nom, s.population,
       ROUND((100 * ST_Area(ST_Intersection(s.geom, z.geom)::geography)
                  / ST_Area(s.geom::geography))::numeric, 1) AS pct_surface_touchee
FROM zones_impact z
JOIN secteurs s ON ST_Intersects(s.geom, z.geom)
WHERE z.niveau = 'SEVERE'
  AND z.alerte_id = (SELECT max(id) FROM alertes_sismiques)
ORDER BY pct_surface_touchee DESC;

-- Q2. K-NN avec LATERAL : les 3 hôpitaux les plus proches POUR CHAQUE alerte
--     (une seule requête, l'index GIST est utilisé via l'opérateur <->).
SELECT a.id, a.magnitude, h.nom, ROUND(h.dist_km::numeric, 1) AS dist_km
FROM (SELECT * FROM alertes_sismiques ORDER BY id DESC LIMIT 10) a
CROSS JOIN LATERAL (
    SELECT nom, ST_Distance(h.geom::geography, a.geom::geography) / 1000 AS dist_km
    FROM hopitaux h
    ORDER BY h.geom <-> a.geom
    LIMIT 3
) h
ORDER BY a.id DESC, h.dist_km;

-- Q3. Regroupement spatial (DBSCAN) : séisme principal + répliques = un événement.
--     eps = 15 km, calcul en mètres dans EPSG:32629 (UTM 29N).
WITH r AS (
    SELECT id, magnitude, geom,
           ST_ClusterDBSCAN(ST_Transform(geom, 32629), eps := 15000, minpoints := 2) OVER () AS cid
    FROM alertes_sismiques
)
SELECT cid, COUNT(*) AS nb_alertes, MAX(magnitude) AS mag_max,
       ROUND((ST_Area(ST_ConvexHull(ST_Collect(geom))::geography) / 1e6)::numeric, 1) AS aire_km2
FROM r WHERE cid IS NOT NULL GROUP BY cid ORDER BY nb_alertes DESC;

-- Q4. Diagramme de Voronoï : zone de responsabilité de chaque hôpital
--     (tout point de la cellule est plus proche de cet hôpital que des autres).
WITH v AS (
    SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom), 0,
            ST_MakeEnvelope(-8.5, 32.8, -6.2, 34.5, 4326)))).geom AS geom
    FROM hopitaux)
SELECT h.nom, ROUND((ST_Area(ST_Intersection(v.geom, ST_MakeEnvelope(-8.5, 32.8, -6.2, 34.5, 4326))::geography) / 1e6)::numeric, 0) AS aire_km2
FROM v JOIN hopitaux h ON ST_Contains(v.geom, h.geom)
ORDER BY aire_km2 DESC;

-- Q5. Hôpitaux situés dans la zone sévère d'une alerte (donc considérés hors service).
SELECT a.id AS alerte, a.magnitude, h.nom
FROM alertes_sismiques a
JOIN zones_impact z ON z.alerte_id = a.id AND z.niveau = 'SEVERE'
JOIN hopitaux h ON ST_Within(h.geom, z.geom)
ORDER BY a.id DESC;

-- Q6. Déserts hospitaliers : secteurs dont le centre est à plus de 15 km de tout hôpital.
SELECT s.nom, s.population
FROM secteurs s
WHERE NOT EXISTS (
    SELECT 1 FROM hopitaux h
    WHERE ST_DWithin(ST_Centroid(s.geom)::geography, h.geom::geography, 15000))
ORDER BY s.population DESC;

-- Q7. Fonction de fenêtrage : temps et distance entre deux alertes successives.
SELECT id, magnitude,
       timestamp - LAG(timestamp) OVER w AS ecart_temps,
       ROUND((ST_Distance(geom::geography, LAG(geom) OVER w ::geography) / 1000)::numeric, 1) AS ecart_km
FROM alertes_sismiques
WINDOW w AS (ORDER BY timestamp)
ORDER BY id DESC LIMIT 10;

-- Q8. Agrégat géométrique : surface totale touchée (union des zones sévères, sans doublons).
SELECT COUNT(*) AS nb_alertes,
       ROUND((ST_Area(ST_Union(geom)::geography) / 1e6)::numeric, 1) AS aire_union_km2,
       ROUND((SUM(ST_Area(geom::geography)) / 1e6)::numeric, 1)      AS somme_aires_km2
FROM zones_impact WHERE niveau = 'SEVERE';

-- Q9. Distance de chaque alerte à la faille la plus proche (K-NN sur lignes).
SELECT a.id, a.magnitude, f.nom,
       ROUND((ST_Distance(f.geom::geography, a.geom::geography) / 1000)::numeric, 1) AS dist_faille_km
FROM alertes_sismiques a
CROSS JOIN LATERAL (SELECT * FROM failles ORDER BY geom <-> a.geom LIMIT 1) f
ORDER BY a.id DESC LIMIT 10;
