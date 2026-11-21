import { Injectable, OnModuleDestroy, OnModuleInit, BadRequestException } from '@nestjs/common';
import { Client, Pool } from 'pg';
import { Subject } from 'rxjs';

const parse = (s: string | null) => (s ? JSON.parse(s) : null);

@Injectable()
export class AppService implements OnModuleInit, OnModuleDestroy {
  private pool: Pool;
  private dbConfig: any;
  private listener?: Client;
  private closing = false;

  /** Émet l'id de chaque nouvelle alerte (alimenté par LISTEN/NOTIFY PostgreSQL). */
  readonly nouvellesAlertes$ = new Subject<number>();

  constructor() {
    const password = process.env.POSTGRES_PASSWORD;
    if (!password) {
      throw new Error('POSTGRES_PASSWORD environment variable is required');
    }
    this.dbConfig = {
      user: process.env.POSTGRES_USER || 'user',
      host: process.env.POSTGRES_HOST || 'localhost',
      database: process.env.POSTGRES_DB || 'georescue',
      password,
      port: Number(process.env.POSTGRES_PORT || 5435),
    };
    this.pool = new Pool(this.dbConfig);
  }

  // ---------- Temps réel : LISTEN / NOTIFY ----------
  async onModuleInit() {
    await this.connectListener();
  }

  async onModuleDestroy() {
    this.closing = true;
    await this.listener?.end().catch(() => undefined);
    await this.pool.end();
  }

  private async connectListener() {
    if (this.closing) return;
    const client = new Client(this.dbConfig);
    const retry = () => {
      client.removeAllListeners();
      client.end().catch(() => undefined);
      if (!this.closing) setTimeout(() => this.connectListener(), 3000);
    };
    client.on('notification', (m) => {
      const id = Number(m.payload);
      if (Number.isInteger(id)) this.nouvellesAlertes$.next(id);
    });
    client.on('error', retry);
    client.on('end', retry);
    try {
      await client.connect();
      await client.query('LISTEN nouvelle_alerte');
      this.listener = client;
      console.log('📡 Écoute PostgreSQL active (canal nouvelle_alerte)');
    } catch {
      retry();
    }
  }

  // ---------- Couches de référence ----------
  async getHopitaux() {
    const res = await this.pool.query(
      'SELECT id, nom, capacite, lits_disponibles, ST_AsGeoJSON(geom) AS geom FROM hopitaux ORDER BY id;',
    );
    return res.rows.map((r) => ({ ...r, geom: parse(r.geom) }));
  }

  async getSecteurs() {
    const res = await this.pool.query(`
      SELECT s.id, s.nom, s.population, ST_AsGeoJSON(s.geom) AS geom,
             (SELECT COUNT(*)::int FROM zones_impact z
               JOIN alertes_sismiques a ON a.id = z.alerte_id
              WHERE z.niveau = 'SEVERE' AND a.timestamp > now() - interval '24 hours'
                AND ST_Intersects(s.geom, z.geom)) AS nb_alertes_severes_24h
      FROM secteurs s ORDER BY s.id;`);
    return res.rows.map((r) => ({ ...r, geom: parse(r.geom) }));
  }

  async getFailles() {
    const res = await this.pool.query('SELECT id, nom, ST_AsGeoJSON(geom) AS geom FROM failles;');
    return res.rows.map((r) => ({ ...r, geom: parse(r.geom) }));
  }

  /** Diagramme de Voronoï : zone de responsabilité de chaque hôpital. */
  async getVoronoi() {
    const res = await this.pool.query(`
      WITH v AS (
        SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom), 0,
                ST_MakeEnvelope(-8.5, 32.8, -6.2, 34.5, 4326)))).geom AS geom
        FROM hopitaux)
      SELECT h.id, h.nom, ST_AsGeoJSON(ST_Intersection(v.geom, ST_MakeEnvelope(-8.5, 32.8, -6.2, 34.5, 4326))) AS geom,
             ROUND((ST_Area(ST_Intersection(v.geom, ST_MakeEnvelope(-8.5, 32.8, -6.2, 34.5, 4326))::geography) / 1e6)::numeric, 0)::int AS aire_km2
      FROM v JOIN hopitaux h ON ST_Contains(v.geom, h.geom);`);
    return res.rows.map((r) => ({ ...r, geom: parse(r.geom) }));
  }

  // ---------- Alertes (zones + dispatch) ----------
  async getAlertes(limit = 10, id?: number) {
    const lim = Math.min(Math.max(limit || 10, 1), 50);
    const alertes = (
      await this.pool.query(
        `SELECT a.id, a.magnitude, a.timestamp, a.source, a.population_severe,
                a.population_totale, a.victimes_estimees, a.deficit_lits,
                ST_AsGeoJSON(a.geom) AS geom, f.nom AS faille_nom, f.dist_km AS faille_km
           FROM alertes_sismiques a
           LEFT JOIN LATERAL (
              SELECT nom, ST_Distance(f.geom::geography, a.geom::geography) / 1000 AS dist_km
                FROM failles f ORDER BY f.geom <-> a.geom LIMIT 1) f ON TRUE
          WHERE ($2::int IS NULL OR a.id = $2)
          ORDER BY a.timestamp DESC, a.id DESC LIMIT $1`,
        [lim, id ?? null],
      )
    ).rows;
    if (!alertes.length) return [];
    const ids = alertes.map((a) => a.id);

    const zones = (
      await this.pool.query(
        `SELECT alerte_id, niveau, rayon_km, population, ST_AsGeoJSON(geom) AS geom
           FROM zones_impact WHERE alerte_id = ANY($1)`,
        [ids],
      )
    ).rows;
    const dispatches = (
      await this.pool.query(
        `SELECT d.alerte_id, d.rang, d.distance_km, d.patients_assignes, d.dans_zone_severe,
                h.id AS hopital_id, h.nom, h.lits_disponibles, ST_AsGeoJSON(h.geom) AS geom
           FROM dispatches d JOIN hopitaux h ON h.id = d.hopital_id
          WHERE d.alerte_id = ANY($1) ORDER BY d.alerte_id, d.rang`,
        [ids],
      )
    ).rows;

    return alertes.map((a) => ({
      ...a,
      geom: parse(a.geom),
      zones: zones
        .filter((z) => z.alerte_id === a.id)
        .map((z) => ({ ...z, geom: parse(z.geom) })),
      dispatches: dispatches
        .filter((d) => d.alerte_id === a.id)
        .map((d) => ({ ...d, geom: parse(d.geom) })),
    }));
  }

  /** Regroupe les alertes de la dernière heure en événements (ST_ClusterDBSCAN). */
  async getClusters() {
    const res = await this.pool.query(`
      WITH r AS (
        SELECT id, magnitude, geom,
               ST_ClusterDBSCAN(ST_Transform(geom, 32629), eps := 15000, minpoints := 2) OVER () AS cid
        FROM (SELECT * FROM alertes_sismiques
               WHERE timestamp > now() - interval '1 hour'
               ORDER BY timestamp DESC LIMIT 300) t)
      SELECT cid, COUNT(*)::int AS nb, MAX(magnitude) AS mag_max,
             ST_AsGeoJSON(ST_Buffer(ST_ConvexHull(ST_Collect(geom))::geography, 1500)::geometry) AS geom
      FROM r WHERE cid IS NOT NULL GROUP BY cid ORDER BY nb DESC;`);
    return res.rows.map((r) => ({ ...r, geom: parse(r.geom) }));
  }

  async getStats() {
    const res = await this.pool.query(`
      SELECT COUNT(*)::int AS nb_1h, COALESCE(MAX(magnitude), 0) AS mag_max_1h
        FROM alertes_sismiques WHERE timestamp > now() - interval '1 hour';`);
    const lits = await this.pool.query(
      'SELECT COALESCE(SUM(lits_disponibles),0)::int AS lits_total, COUNT(*)::int AS nb_hopitaux FROM hopitaux;',
    );
    return { ...res.rows[0], ...lits.rows[0] };
  }

  /** Crée une alerte manuelle : le trigger SQL calcule zones, population et dispatch. */
  async createScenario(lat: number, lon: number, magnitude: number) {
    const res = await this.pool.query(
      `INSERT INTO alertes_sismiques (magnitude, geom, source)
       VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326), 'SCENARIO') RETURNING id`,
      [magnitude, lon, lat],
    );
    return { id: res.rows[0].id as number };
  }

  async getNearestHopitaux(lat: number, lon: number) {
    if (![lat, lon].every(Number.isFinite) || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
      throw new BadRequestException('lat/lon invalides');
    }
    const res = await this.pool.query(
      `SELECT nom, ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) / 1000 AS distance_km
         FROM hopitaux ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326) LIMIT 3;`,
      [lon, lat],
    );
    return res.rows;
  }
}