import { registerAs } from '@nestjs/config';

export default registerAs('server', () => ({
  host: process.env.SERVER_HOST ?? '0.0.0.0',
  port: Number(process.env.SERVER_PORT ?? 3000),
}));
