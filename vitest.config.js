import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: false,
    silent: false,
    environment: "node",
    testTimeout: 10000,
    hookTimeout: 10000,
    env: {
      RABBIT_URL: process.env.RABBIT_URL || "amqp://localhost"
    },
    execArgv: ["--trace-deprecation"]
  }
});
