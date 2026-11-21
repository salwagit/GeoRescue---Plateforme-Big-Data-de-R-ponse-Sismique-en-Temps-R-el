-- =====================================================================
-- GeoRescue : schéma spatial v2
-- Exécuté au 1er démarrage du conteneur (voir README : docker compose down -v
-- pour réinitialiser une base existante).
-- Données de démonstration : secteurs, populations, lits et faille sont FICTIFS.
-- =====================================================================
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------- 1. TABLES -------------------------------------------------
CREATE TABLE secteurs (                         -- polygones (jointures spatiales)
    id          SERIAL PRIMARY KEY,
    nom         VARCHAR(100) NOT NULL,
    population  INT NOT NULL CHECK (population >= 0),
    geom        GEOMETRY(Polygon, 4326) NOT NULL CHECK (ST_IsValid(geom))
);

CREATE TABLE hopitaux (                         -- points
    id                SERIAL PRIMARY KEY,
    nom               VARCHAR(255) NOT NULL UNIQUE,
    capacite          INT NOT NULL CHECK (capacite > 0),
    lits_disponibles  INT NOT NULL CHECK (lits_disponibles >= 0),
    geom              GEOMETRY(Point, 4326) NOT NULL,
    CHECK (lits_disponibles <= capacite)
);

CREATE TABLE failles (                          -- lignes
    id    SERIAL PRIMARY KEY,
    nom   VARCHAR(100) NOT NULL,
    geom  GEOMETRY(LineString, 4326) NOT NULL
);

CREATE TABLE alertes_sismiques (
    id                  SERIAL PRIMARY KEY,
    magnitude           REAL NOT NULL CHECK (magnitude BETWEEN 0 AND 10),
    geom                GEOMETRY(Point, 4326) NOT NULL,
    timestamp           TIMESTAMPTZ NOT NULL DEFAULT now(),
    source              VARCHAR(20) NOT NULL DEFAULT 'SIMULATEUR'
                        CHECK (source IN ('SIMULATEUR', 'SCENARIO', 'USGS')),
    -- colonnes calculées par le trigger
    population_severe   INT,
    population_totale   INT,
    victimes_estimees   INT,
    deficit_lits        INT
);

CREATE TABLE zones_impact (                     -- 3 polygones par alerte
    id          SERIAL PRIMARY KEY,
    alerte_id   INT NOT NULL REFERENCES alertes_sismiques(id) ON DELETE CASCADE,
    niveau      VARCHAR(10) NOT NULL CHECK (niveau IN ('SEVERE', 'MODERE', 'LEGER')),
    rayon_km    REAL NOT NULL,
    population  INT NOT NULL,
    geom        GEOMETRY(Polygon, 4326) NOT NULL,
    UNIQUE (alerte_id, niveau)
);

CREATE TABLE dispatches (                       -- affectation alerte -> hôpital
    id                 SERIAL PRIMARY KEY,
    alerte_id          INT NOT NULL REFERENCES alertes_sismiques(id) ON DELETE CASCADE,
    hopital_id         INT NOT NULL REFERENCES hopitaux(id),
    rang               INT NOT NULL,
    distance_km        DOUBLE PRECISION NOT NULL,
    patients_assignes  INT NOT NULL DEFAULT 0,
    dans_zone_severe   BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (alerte_id, hopital_id)
);

-- ---------- 2. INDEX --------------------------------------------------
CREATE INDEX idx_secteurs_geom  ON secteurs          USING GIST (geom);
CREATE INDEX idx_hopitaux_geom  ON hopitaux          USING GIST (geom);
CREATE INDEX idx_failles_geom   ON failles           USING GIST (geom);
CREATE INDEX idx_alertes_geom   ON alertes_sismiques USING GIST (geom);
CREATE INDEX idx_zones_geom     ON zones_impact      USING GIST (geom);
CREATE INDEX idx_alertes_ts     ON alertes_sismiques (timestamp DESC);
CREATE INDEX idx_zones_alerte   ON zones_impact (alerte_id);
CREATE INDEX idx_disp_alerte    ON dispatches (alerte_id);

-- ---------- 3. FONCTIONS MÉTIER (PL/pgSQL) ----------------------------
-- Rayon d'une zone d'impact selon la magnitude (modèle empirique simplifié).
CREATE OR REPLACE FUNCTION rayon_impact_km(mag REAL, niveau TEXT)
RETURNS REAL LANGUAGE sql IMMUTABLE AS $$
    SELECT (1.5 * power(10, 0.30 * (mag - 4)) *
            CASE niveau WHEN 'SEVERE' THEN 1.0 WHEN 'MODERE' THEN 2.5 ELSE 5.0 END)::REAL;
$$;

-- Génère les 3 zones d'impact (buffers géographiques) et estime la population
-- exposée : population du secteur x (surface d'intersection / surface du secteur).
CREATE OR REPLACE FUNCTION calculer_impact(p_alerte_id INT) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    a RECORD; niv TEXT; r REAL; g GEOMETRY; pop INT;
    pop_sev INT := 0; pop_mod INT := 0; pop_tot INT := 0; victimes INT;
BEGIN
    SELECT * INTO a FROM alertes_sismiques WHERE id = p_alerte_id;
    DELETE FROM zones_impact WHERE alerte_id = p_alerte_id;

    FOREACH niv IN ARRAY ARRAY['SEVERE', 'MODERE', 'LEGER'] LOOP
        r := rayon_impact_km(a.magnitude, niv);
        g := ST_Buffer(a.geom::geography, r * 1000)::geometry;

        SELECT COALESCE(SUM(s.population *
               ST_Area(ST_Intersection(s.geom, g)::geography) /
               NULLIF(ST_Area(s.geom::geography), 0)), 0)::INT
        INTO pop FROM secteurs s WHERE ST_Intersects(s.geom, g);

        INSERT INTO zones_impact (alerte_id, niveau, rayon_km, population, geom)
        VALUES (p_alerte_id, niv, r, pop, g);

        IF niv = 'SEVERE' THEN pop_sev := pop;
        ELSIF niv = 'MODERE' THEN pop_mod := pop;
        ELSE pop_tot := pop; END IF;
    END LOOP;

    -- Victimes estimées (modèle fictif de démonstration)
    victimes := ROUND((pop_sev + 0.2 * GREATEST(pop_mod - pop_sev, 0))
                      * 0.003 * power(GREATEST(a.magnitude - 3, 0), 2));

    UPDATE alertes_sismiques
       SET population_severe = pop_sev, population_totale = pop_tot,
           victimes_estimees = victimes
     WHERE id = p_alerte_id;
END $$;

-- Affecte les blessés aux hôpitaux : parcours K-NN (opérateur <->) ; un hôpital
-- situé dans la zone sévère est considéré hors service (0 patient) ; chaque autre
-- hôpital reçoit au plus ses lits disponibles. Le reste = déficit de lits.
CREATE OR REPLACE FUNCTION affecter_secours(p_alerte_id INT) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    a RECORD; h RECORD; zone_sev GEOMETRY; reste INT; aff INT; k INT := 0;
BEGIN
    SELECT * INTO a FROM alertes_sismiques WHERE id = p_alerte_id;
    SELECT geom INTO zone_sev FROM zones_impact
     WHERE alerte_id = p_alerte_id AND niveau = 'SEVERE';
    DELETE FROM dispatches WHERE alerte_id = p_alerte_id;
    reste := COALESCE(a.victimes_estimees, 0);

    FOR h IN
        SELECT id, lits_disponibles, ST_Intersects(geom, zone_sev) AS touche,
               ST_Distance(geom::geography, a.geom::geography) / 1000 AS d
          FROM hopitaux
         ORDER BY geom <-> a.geom
         LIMIT 8
    LOOP
        k := k + 1;
        aff := CASE WHEN h.touche THEN 0 ELSE LEAST(reste, h.lits_disponibles) END;
        INSERT INTO dispatches (alerte_id, hopital_id, rang, distance_km, patients_assignes, dans_zone_severe)
        VALUES (p_alerte_id, h.id, k, h.d, aff, h.touche);
        reste := reste - aff;
        EXIT WHEN reste <= 0 AND k >= 3;
    END LOOP;

    UPDATE alertes_sismiques SET deficit_lits = GREATEST(reste, 0) WHERE id = p_alerte_id;
END $$;

-- ---------- 4. TRIGGER : tout se calcule à l'insertion d'une alerte ----
CREATE OR REPLACE FUNCTION trg_traiter_alerte() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM calculer_impact(NEW.id);
    PERFORM affecter_secours(NEW.id);
    PERFORM pg_notify('nouvelle_alerte', NEW.id::text);  -- temps réel (LISTEN/NOTIFY)
    RETURN NULL;
END $$;

CREATE TRIGGER trg_alerte_apres_insert
AFTER INSERT ON alertes_sismiques
FOR EACH ROW EXECUTE FUNCTION trg_traiter_alerte();

-- ---------- 5. DONNÉES DE DÉMONSTRATION -------------------------------
-- Coordonnées approximatives ; capacités et lits fictifs.
INSERT INTO hopitaux (nom, capacite, lits_disponibles, geom) VALUES
('CHU Ibn Rochd Casablanca',     1000, 120, ST_SetSRID(ST_MakePoint(-7.618, 33.589), 4326)),
('Hôpital Cheikh Khalifa',        300,  60, ST_SetSRID(ST_MakePoint(-7.657, 33.558), 4326)),
('Hôpital Moulay Youssef',        150,  35, ST_SetSRID(ST_MakePoint(-7.625, 33.600), 4326)),
('Hôpital Militaire Rabat',       800, 150, ST_SetSRID(ST_MakePoint(-6.840, 33.980), 4326)),
('Hôpital Provincial Mohammedia', 220,  40, ST_SetSRID(ST_MakePoint(-7.383, 33.686), 4326)),
('Hôpital Provincial Berrechid',  180,  30, ST_SetSRID(ST_MakePoint(-7.587, 33.265), 4326)),
('Hôpital Hassan II Settat',      400,  70, ST_SetSRID(ST_MakePoint(-7.617, 33.001), 4326)),
('Hôpital Ain Chock (démo)',      250,  45, ST_SetSRID(ST_MakePoint(-7.600, 33.540), 4326)),
('Clinique Anfa (démo)',          120,  25, ST_SetSRID(ST_MakePoint(-7.660, 33.585), 4326)),
('Hôpital Sidi Moumen (démo)',    200,  40, ST_SetSRID(ST_MakePoint(-7.500, 33.600), 4326));

-- Grille de 7 x 5 secteurs (0,1° de côté) ; densité fictive décroissante
-- depuis le centre de Casablanca.
INSERT INTO secteurs (nom, population, geom)
SELECT 'Secteur ' || chr(65 + j) || (i + 1),
       (3000 + 140000 * exp(-sqrt(power(lon + 0.05 + 7.62, 2) + power(lat + 0.05 - 33.58, 2)) / 0.12))::INT,
       ST_MakeEnvelope(lon, lat, lon + 0.1, lat + 0.1, 4326)
FROM (SELECT i, j, -7.95 + i * 0.1 AS lon, 33.25 + j * 0.1 AS lat
        FROM generate_series(0, 6) i, generate_series(0, 4) j) g;

INSERT INTO failles (nom, geom) VALUES
('Faille de démonstration',
 ST_GeomFromText('LINESTRING(-7.95 33.35, -7.75 33.45, -7.55 33.60, -7.30 33.72)', 4326));