import { Logger as NestLogger } from '@nestjs/common';
import { Logger as TypeOrmLogger } from 'typeorm';

export class DatabaseLogger implements TypeOrmLogger {
  private readonly logger = new NestLogger('Database');

  logQuery(query: string, parameters?: unknown[]) {
    this.logger.debug(
      {
        query,
        parameters,
      },
      'TypeORM query',
    );
  }

  logQueryError(err: string, query: string, parameters?: unknown[]) {
    this.logger.error(
      {
        err,
        query,
        parameters,
      },
      'TypeORM error',
    );
  }

  logQuerySlow(time: number, query: string, parameters?: unknown[]) {
    this.logger.warn(
      {
        time,
        query,
        parameters,
      },
      'TypeORM slow query',
    );
  }

  logMigration(message: string) {
    this.logger.debug(
      {
        message,
      },
      'TypeORM log migration',
    );
  }

  logSchemaBuild(message: string) {
    this.logger.debug(
      {
        message,
      },
      'TypeORM schema build',
    );
  }

  log(level: 'log' | 'info' | 'warn', message: string) {
    if (level === 'log') {
      return this.logger.log(message);
    }
    if (level === 'info') {
      return this.logger.debug(message);
    }
    if (level === 'warn') {
      return this.logger.warn(message);
    }
  }
}
