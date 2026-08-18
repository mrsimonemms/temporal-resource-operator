import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule, TypeOrmModuleOptions } from '@nestjs/typeorm';
import { PrometheusModule } from '@willsoto/nestjs-prometheus';

import config from './config';
import { HealthModule } from './health/health.module';
import { DatabaseLogger } from './lib/database';
import { MetricsController } from './health/metrics.controller';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: config,
    }),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService): TypeOrmModuleOptions => {
        const db = configService.getOrThrow<TypeOrmModuleOptions>('db');

        return {
          ...db,
          logger: new DatabaseLogger(),
        };
      },
    }),
    PrometheusModule.register({
      // Define the controller so it's not under /v1 namespaces
      controller: MetricsController,
    }),

    // Load the internal modules
    HealthModule,
  ],
  controllers: [],
  providers: [],
})
export class AppModule {}
