import {
  HealthCheckResult,
  HealthCheckService,
  HealthIndicatorResult,
  TerminusModule,
  TypeOrmHealthIndicator,
} from '@nestjs/terminus';
import { Test, TestingModule } from '@nestjs/testing';

import { HealthController } from './health.controller';

describe('HealthController', () => {
  let controller: HealthController;
  let service: Partial<HealthCheckService>;
  let dbCheck: Partial<TypeOrmHealthIndicator>;

  beforeEach(async () => {
    service = {
      check: jest.fn(),
    };

    dbCheck = {
      pingCheck: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      imports: [TerminusModule],
      controllers: [HealthController],
      providers: [
        {
          provide: HealthCheckService,
          useValue: service,
        },
        {
          provide: TypeOrmHealthIndicator,
          useValue: dbCheck,
        },
      ],
    }).compile();

    controller = module.get<HealthController>(HealthController);
  });

  it('should return a successful result', async () => {
    const serviceResult: HealthCheckResult = {
      status: 'ok',
      details: {},
    };
    const dbResult: HealthIndicatorResult = {
      database: {
        status: 'up',
      },
    };
    const serviceMock = jest
      .spyOn(service, 'check')
      .mockResolvedValue(serviceResult);

    jest.spyOn(dbCheck, 'pingCheck').mockResolvedValue(dbResult);

    await expect(controller.check()).resolves.toBe(serviceResult);

    expect(service.check).toHaveBeenCalled();

    await expect(serviceMock.mock.calls[0][0][0]()).resolves.toBe(dbResult);
  });
});
