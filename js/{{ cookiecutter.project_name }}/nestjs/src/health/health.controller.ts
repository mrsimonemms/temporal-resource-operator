import { Controller, Get, Inject, VERSION_NEUTRAL } from '@nestjs/common';
import {
  HealthCheck,
  HealthCheckService,
  TypeOrmHealthIndicator,
} from '@nestjs/terminus';

@Controller({
  path: 'health',
  version: VERSION_NEUTRAL,
})
export class HealthController {
  @Inject(TypeOrmHealthIndicator)
  private readonly db: TypeOrmHealthIndicator;

  @Inject(HealthCheckService)
  private readonly health: HealthCheckService;

  @Get()
  @HealthCheck()
  check() {
    // Allow 1 second before timeout
    const timeout = 1000;

    return this.health.check([
      () =>
        this.db.pingCheck('database', {
          timeout,
        }),
    ]);
  }
}
