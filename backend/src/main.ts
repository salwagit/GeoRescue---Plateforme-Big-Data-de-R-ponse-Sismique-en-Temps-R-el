import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'path';
import { config } from 'dotenv';

config({ path: join(process.cwd(), '..', '.env') });

async function bootstrap() {
  const { AppModule } = await import('./app.module');
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  app.useStaticAssets(join(__dirname, '..', 'public'));
  app.enableCors();
  await app.listen(3000);
  console.log('🌍 GeoRescue API en cours d\'exécution sur http://localhost:3000');
}
bootstrap();
