// Runs every failure scenario in fault-worker.mjs against both libraries, each in its own process,
// and prints the results side by side. `node faults.mjs` after `node setup.mjs`, ElasticMQ on :9324.
import { spawnSync } from 'node:child_process';

const SCENARIOS = [
  'shutdown',
  'outage',
  'deleteHiccup',
  'fifo',
  'wrappedAfterBoot',
  'eventHandlerInstance',
];
// Outage and delete failures also run as apps often do: with unhandled rejections only logged
// (SURVIVE), and with an `error` event handler registered (ERROR_HANDLER).
const MODES = {
  default: {},
  SURVIVE: { SURVIVE: '1' },
  ERROR_HANDLER: { ERROR_HANDLER: '1' },
};
const MODE_SCENARIOS = new Set(['outage', 'deleteHiccup']);
const LIBRARIES = ['@ssut/nestjs-sqs', 'nest-sqs'];

for (const scenario of SCENARIOS) {
  for (const [mode, env] of Object.entries(MODES)) {
    if (mode !== 'default' && !MODE_SCENARIOS.has(scenario)) continue;
    for (const library of LIBRARIES) {
      const run = spawnSync('node', ['fault-worker.mjs', library, scenario], {
        cwd: import.meta.dirname,
        encoding: 'utf8',
        timeout: 60_000,
        env: { ...process.env, ...env },
      });
      const line = run.stdout.trim().split('\n').at(-1);
      let result;
      try {
        result = JSON.parse(line);
      } catch {
        const reason = run.signal ? `killed (${run.signal})` : `exit ${run.status}`;
        const error =
          run.stderr
            .trim()
            .split('\n')
            .find((text) => /Error|error/.test(text)) ?? '';
        result = { crashed: `${reason} ${error.slice(0, 120)}` };
      }
      console.log(
        `${scenario.padEnd(22)} ${mode.padEnd(14)} ${library.padEnd(18)} ${JSON.stringify(result)}`,
      );
    }
  }
}
