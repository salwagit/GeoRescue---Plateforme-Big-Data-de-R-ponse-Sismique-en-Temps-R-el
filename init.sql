
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE hopitaux (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(255),
    capacite INT,
    geom GEOMETRY(Point, 4326)
);

CREATE TABLE alertes_sismiques (
    id SERIAL PRIMARY KEY,
    magnitude FLOAT,
    geom GEOMETRY(Point, 4326),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index Spatiaux (GIST)
CREATE INDEX idx_hopitaux_geom ON hopitaux USING GIST (geom);
CREATE INDEX idx_alertes_geom ON alertes_sismiques USING GIST (geom);

-- Insertion de données de test (Hôpitaux à Casablanca et alentours)
INSERT INTO hopitaux (nom, capacite, geom) VALUES
('CHU Ibn Rochd Casablanca', 1000, ST_SetSRID(ST_MakePoint(-7.618, 33.589), 4326)),
('Hôpital Cheikh Khalifa', 300, ST_SetSRID(ST_MakePoint(-7.657, 33.558), 4326)),
('Hôpital Moulay Youssef', 150, ST_SetSRID(ST_MakePoint(-7.625, 33.600), 4326)),
('Hôpital Militaire Rabat', 800, ST_SetSRID(ST_MakePoint(-6.840, 33.980), 4326));
