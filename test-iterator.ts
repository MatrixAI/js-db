import { withF } from '@matrixai/resources';
import { DB } from './src';
import * as testsUtils from './tests/utils';

async function main () {

  const key = await testsUtils.generateKey(256);

  const db = await DB.createDB({
    dbPath: './tmp/db',
    crypto: {
      key,
      ops: { encrypt: testsUtils.encrypt, decrypt: testsUtils.decrypt },
    },
    fresh: true
  });

  await db.put('key', 'value');

  const p = db.put('key', 'value3');

  const t1 = withF([db.transaction()], async ([tran]) => {
    const i1 = tran.iterator({ keyAsBuffer: false, valueAsBuffer: false });

    console.log('CREATED ITERATOR');

    i1.seek('key')
    const v1 = (await i1.next())![1];
    await i1.end();

    const v2 = await tran.get('key');

    console.log(v1, v2, v1 === v2);
  });

  await p;
  await t1;

  const t2 = withF([db.transaction()], async ([tran]) => {
    console.log(await tran.get('key'));
  });

  await t2;

  await db.stop();
}

main();
