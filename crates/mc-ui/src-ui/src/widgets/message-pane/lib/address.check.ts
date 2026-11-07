// Run with `bun run src/widgets/message-pane/lib/address.check.ts`.
import { parseAddress } from './address';

const eq = (actual: unknown, expected: unknown, what: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
};

eq(parseAddress('Anna Rossi <anna@x.it>'), { name: 'Anna Rossi', email: 'anna@x.it' }, 'name and address');
eq(parseAddress('"Rossi, Anna" <anna@x.it>'), { name: 'Rossi, Anna', email: 'anna@x.it' }, 'quoted name');
eq(parseAddress(' anna@x.it '), { name: '', email: 'anna@x.it' }, 'bare');
eq(parseAddress('<anna@x.it>'), { name: '', email: 'anna@x.it' }, 'angle only');
