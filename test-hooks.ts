import type { LevelDB } from 'level';
import type { AbstractLevelDOWN, AbstractIterator, AbstractBatch, AbstractOptions } from 'abstract-leveldown';

import level from 'level';
import hookdown from 'level-hookdown';
import subleveldown from 'subleveldown';
import AutoIndex from 'level-auto-index';
import Index from 'level-idx';

type Callback<P extends Array<any> = [], R = any, E extends Error = Error> = {
  (e: E, ...params: Partial<P>): R;
  (e?: null | undefined, ...params: P): R;
};

type HookOp<K = any, V = any> = {
  type: 'put';
  key: K;
  value: V;
  opts: AbstractOptions;
} | {
  type: 'del';
  key: K;
  opts: AbstractOptions;
} | {
  type: 'batch',
  array: Array<HookOp<K, V>>;
  opts: AbstractOptions;
};

interface LevelDBHooked<K = any, V = any> extends LevelDB<K, V> {
  prehooks: Array<(op: HookOp<K, V>, cb: Callback) => void>;
  posthooks: Array<(op: HookOp<K, V>, cb: Callback) => void>;
}

async function main () {

  // all buffers are Uint8Arrays and all Uint8Arrays are ArrayBuffer

  const db = await new Promise<LevelDB<Buffer | string, Buffer | string>>((resolve, reject) => {
    const db = level(
      './tmp/db',
      {
        keyEncoding: 'binary',
        valueEncoding: 'binary'
      },
      (e) => {
        if (e) {
          reject(e);
        } else {
          resolve(db);
        }
      }
    );
  });

  // db.put('abc', 'blah', {

  // });

  // so there's a problem
  // can these hooks change the operation?
  // like the `op` is an object

  const hookdb = hookdown(db) as LevelDBHooked;

  // console.log(hookdb);

  const prehook1 = (op: HookOp, cb: Callback) => {
    console.log('pre1', op);
    cb();
  };
  const prehook2 = (op: HookOp, cb: Callback) => {
    console.log('pre2', op);
    cb();
  };

  const posthook1 = (op: HookOp, cb: Callback) => {
    console.log('post1', op);
    cb();
  };

  const posthook2 = (op: HookOp, cb: Callback) => {
    console.log('post2', op);
    cb();
  };

  hookdb.prehooks.push(prehook1);
  hookdb.prehooks.push(prehook2);

  hookdb.posthooks.push(posthook1);
  hookdb.posthooks.push(posthook2);

  await db.put('beep', 'boop');

  // await db.del('beep');
  // await db.batch([
  //   { type: 'put', key: 'fatehr', value: 'gloop' },
  //   { type: 'put', key: 'omther', value: 'what' }
  // ]);

  console.log((await db.get('beep')).toString());

}

main();
