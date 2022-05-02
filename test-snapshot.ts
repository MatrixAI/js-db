import { withF, ResourceRelease } from '@matrixai/resources';
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

  const t1 = withF([db.transaction()], async ([tran]) => {
    await testsUtils.sleep(100);
    console.log('T1: LOADING KEY2', await tran.get('key2'));
  });

  // T2 starts after T1 starts
  const t2 = withF([db.transaction()], async ([tran]) => {
    await tran.put('key2', 'value2');
  });

  await t2;
  await t1;

  const t3 = withF([db.transaction()], async ([tran]) => {
    console.log('T3: LOADING KEY2', await tran.get('key2'));
  });

  await t3;

  await db.stop();

}

main();

// const tranAcquire = db.transaction();
// const [tranRelease, tran] = await tranAcquire() as [ResourceRelease, DBTransaction];
// console.log(await tran.get('key'));
// await tran.del('key');
// console.log(await tran.dump());
// for await (const [k, v] of tran.iterator<string>({ valueAsBuffer: false })) {
//   console.log(k, v);
// }
// await tranRelease();
