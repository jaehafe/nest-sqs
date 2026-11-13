// Handlers stand in for real work with timers, so CPU time is the library's own overhead.
export const SCENARIOS = {
  // Fast handlers: the library's polling and deleting dominate.
  fast: { messages: 3000, slowEvery: 0, slowMs: 0, fastMs: 0 },
  // 1 in 50 messages takes 1 s, the rest 5 ms: shows whether a slow message holds back the others.
  mixed: { messages: 1000, slowEvery: 50, slowMs: 1000, fastMs: 5 },
};

export function handle(scenario, message) {
  const index = Number(message.Body);
  const slow = scenario.slowEvery > 0 && index % scenario.slowEvery === 0;
  const ms = slow ? scenario.slowMs : scenario.fastMs;
  return ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve();
}
