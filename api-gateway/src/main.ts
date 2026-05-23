import { NestFactory } from '@nestjs/core';
import { AppModule } from './app/app.module';

// Producer Side
async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(3000);
  console.log('API Gateway is running on port 3000');
}

bootstrap();
