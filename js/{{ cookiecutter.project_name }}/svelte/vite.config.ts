import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    host: process.env.VITE_HOST,
    port: Number(process.env.VITE_PORT ?? 5173),
  },
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}']
  }
});
