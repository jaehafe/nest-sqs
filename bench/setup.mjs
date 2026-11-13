// Installs the bench dependencies and the current nest-sqs build as a packed tarball, so nest-sqs
// resolves NestJS from this folder (NestJS 11) like an app would, not from the repo's dev copy.
import { execFileSync } from 'node:child_process';
import { readdirSync, rmSync } from 'node:fs';

const run = (cmd, args, cwd = import.meta.dirname) =>
  execFileSync(cmd, args, { cwd, stdio: 'inherit' });
const root = new URL('..', import.meta.url).pathname;

run('pnpm', ['run', 'build'], root);
for (const file of readdirSync(import.meta.dirname)) {
  if (file.endsWith('.tgz')) rmSync(new URL(file, import.meta.url));
}
run('npm', ['pack', '--pack-destination', import.meta.dirname], root);
const tarball = readdirSync(import.meta.dirname).find((file) => file.endsWith('.tgz'));
run('npm', ['install', '--no-audit', '--no-fund']);
run('npm', ['install', '--no-save', '--no-audit', '--no-fund', `./${tarball}`]);
