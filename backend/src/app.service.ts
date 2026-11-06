import { Injectable } from '@nestjs/common';
import { Pool } from 'pg';

@Injectable()
export class AppService {
  private pool: Pool;

  constructor() {
    const password = process.env.POSTGRES_PASSWORD;
    if (!password) {
      throw new Error('POSTGRES_PASSWORD environment variable is required');
    }

    this.pool = new Pool({
      user: process.env.POSTGRES_USER || 'user',
      host: process.env.POSTGRES_HOST || 'localhost',
      database: process.env.POSTGRES_DB || 'georescue',
      password,
      port: Number(process.env.POSTGRES_PORT || 5435),
    });
  }

  async getHopitaux() {
    const query = 'SELECT id, nom, capacite, ST_AsGeoJSON(geom) as geom FROM hopitaux;';
    const res = await this.pool.query(query);
    return res.rows.map(row => ({ ...row, geom: JSON.parse(row.geom) }));
  }

  async getDernieresAlertes() {
    // Récupère les alertes avec un Buffer de 20km (ST_Buffer) généré côté base de données
    const query = `
      SELECT id, magnitude, 
             ST_AsGeoJSON(geom) as geom, 
             ST_AsGeoJSON(ST_Buffer(geom::geography, 20000)::geometry) as buffer 
      FROM alertes_sismiques 
      ORDER BY timestamp DESC LIMIT 5;
    `;
    const res = await this.pool.query(query);
    return res.rows.map(row => ({ 
      ...row, 
      geom: JSON.parse(row.geom),
      buffer: JSON.parse(row.buffer)
    }));
  }

  async getNearestHopitaux(lat: number, lon: number) {
    // Utilisation de l'index K-NN (Opérateur <->) pour trouver les 3 hôpitaux les plus proches
    const query = `
      SELECT nom, 
             ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) / 1000 AS distance_km 
      FROM hopitaux 
      ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326) 
      LIMIT 3;
    `;
    const res = await this.pool.query(query, [lon, lat]);
    return res.rows;
  }
}
