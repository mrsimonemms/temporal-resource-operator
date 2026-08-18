import { Controller, Get, Res, VERSION_NEUTRAL } from '@nestjs/common';
import { FastifyReply } from 'fastify';
import * as client from 'prom-client';

@Controller({
  version: VERSION_NEUTRAL,
})
export class MetricsController {
  @Get()
  async index(
    @Res({ passthrough: true }) response: FastifyReply,
  ): Promise<string> {
    response.header('Content-Type', client.register.contentType);
    return client.register.metrics();
  }
}
