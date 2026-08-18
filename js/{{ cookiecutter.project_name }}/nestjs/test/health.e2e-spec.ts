import { Response } from 'light-my-request';

import { request } from './setup';

describe('HealthController (e2e)', () => {
  let app: Promise<Response>;

  describe('/', () => {
    describe('GET', () => {
      beforeEach(async () => {
        app = (await request()).inject({
          method: 'GET',
          url: '/health',
        });
      });

      it('should return a healthy state', () =>
        app.then((result) => {
          expect(result.statusCode).toEqual(200);
        }));
    });
  });
});
