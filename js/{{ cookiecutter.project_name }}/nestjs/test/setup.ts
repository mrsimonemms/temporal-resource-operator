import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { Test, TestingModule } from '@nestjs/testing';

import { AppModule } from '../src/app.module';

let app: NestFastifyApplication | undefined;

export const destroyApp = async () => {
  if (app) {
    await app.close();

    /* Remove definition to prevent being run again */
    app = undefined;
  }
};

export const request = async (): Promise<NestFastifyApplication> => {
  const moduleRef: TestingModule = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();

  app = moduleRef.createNestApplication<NestFastifyApplication>(
    new FastifyAdapter(),
  );

  await app.init();
  await app.getHttpAdapter().getInstance().ready();

  return app;
};

// Ensure that the test app is correctly destroyed after use
afterEach(() => destroyApp());
