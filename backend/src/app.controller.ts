import { BadRequestException, Body, Controller, Get, MessageEvent, Param, ParseIntPipe, Post, Query, Sse } from '@nestjs/common';
import { Observable, interval, map, merge } from 'rxjs';
import { AppService } from './app.service';

@Controller('api')
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('hopitaux') getHopitaux() { return this.appService.getHopitaux(); }
  @Get('secteurs') getSecteurs() { return this.appService.getSecteurs(); }
  @Get('failles') getFailles() { return this.appService.getFailles(); }
  @Get('voronoi') getVoronoi() { return this.appService.getVoronoi(); }
  @Get('clusters') getClusters() { return this.appService.getClusters(); }
  @Get('stats') getStats() { return this.appService.getStats(); }

  @Get('alertes')
  getAlertes(@Query('limit') limit?: string) {
    return this.appService.getAlertes(parseInt(limit ?? '10', 10));
  }

  @Get('alertes/:id')
  getAlerte(@Param('id', ParseIntPipe) id: number) {
    return this.appService.getAlertes(1, id);
  }

  @Get('dispatch')
  getDispatch(@Query('lat') lat: string, @Query('lon') lon: string) {
    return this.appService.getNearestHopitaux(parseFloat(lat), parseFloat(lon));
  }

  /** Mode scénario : l'utilisateur simule un séisme en cliquant sur la carte. */
  @Post('scenario')
  createScenario(@Body() body: { lat?: unknown; lon?: unknown; magnitude?: unknown }) {
    const lat = Number(body?.lat), lon = Number(body?.lon), mag = Number(body?.magnitude);
    if (![lat, lon, mag].every(Number.isFinite)) throw new BadRequestException('lat, lon et magnitude numériques requis');
    if (Math.abs(lat) > 90 || Math.abs(lon) > 180) throw new BadRequestException('coordonnées hors limites');
    if (mag < 3 || mag > 9) throw new BadRequestException('magnitude entre 3 et 9');
    return this.appService.createScenario(lat, lon, Math.round(mag * 10) / 10);
  }

  /** Flux Server-Sent Events : remplace le polling côté navigateur. */
  @Sse('stream')
  stream(): Observable<MessageEvent> {
    const alertes = this.appService.nouvellesAlertes$.pipe(map((id) => ({ data: { id } }) as MessageEvent));
    const ping = interval(20000).pipe(map(() => ({ data: { ping: true } }) as MessageEvent));
    return merge(alertes, ping);
  }
}