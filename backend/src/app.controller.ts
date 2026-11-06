import { Controller, Get, Query } from '@nestjs/common';
import { AppService } from './app.service';

@Controller('api')
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('hopitaux')
  getHopitaux() {
    return this.appService.getHopitaux();
  }

  @Get('alertes')
  getAlertes() {
    return this.appService.getDernieresAlertes();
  }

  @Get('dispatch')
  getDispatch(@Query('lat') lat: string, @Query('lon') lon: string) {
    return this.appService.getNearestHopitaux(parseFloat(lat), parseFloat(lon));
  }
}
