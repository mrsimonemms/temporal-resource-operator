import { registerAs } from '@nestjs/config';
import { TypeOrmModuleOptions } from '@nestjs/typeorm';
import { join } from 'node:path';
import { LoggerOptions } from 'typeorm/logger/LoggerOptions';

export default registerAs('db', (): TypeOrmModuleOptions => {
  /* SSL configuration should default to undefined */
  let ssl: any;
  if (process.env.DB_USE_SSL === 'true') {
    ssl = {
      ca: process.env.DB_SSL_CA,
      cert: process.env.DB_SSL_CERT,
      key: process.env.DB_SSL_KEY,
    };
  }

  let logging: boolean | string | undefined = false;
  const loggingVar = process.env.DB_LOGGING;

  if (loggingVar === 'true') {
    logging = true;
  } else if (loggingVar === 'false') {
    logging = false;
  } else {
    logging = loggingVar;
  }

  return {
    ssl,
    type: (process.env.DB_TYPE as 'mysql' | 'mariadb') ?? 'mysql',
    host: process.env.DB_HOST,
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: process.env.DB_PORT ? Number(process.env.DB_PORT) : undefined,
    migrationsRun: process.env.DB_MIGRATIONS_RUN !== 'false',
    synchronize: process.env.DB_SYNC === 'true',
    autoLoadEntities: true,
    logging: (logging as LoggerOptions) ?? true,
    migrations: [join(__dirname, '..', 'migrations', '*{.ts,.js}')],
    retryAttempts: Number(process.env.DB_RETRY_ATTEMPTS ?? 10),
    retryDelay: Number(process.env.DB_RETRY_DELAY ?? 3000),
  };
});
