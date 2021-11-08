import type { LevelDB } from 'level';
import type { AbstractLevelDOWN, AbstractIterator, AbstractBatch, AbstractOptions } from 'abstract-leveldown';

import level from 'level';
import hookdown from 'level-hookdown';
import subleveldown from 'subleveldown';
import AutoIndex from 'level-auto-index';
import Index from 'level-idx';
import sublevelprefixer from 'sublevel-prefixer';

const prefixer = sublevelprefixer('!');

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

  const db = await new Promise<LevelDB<string | ArrayBuffer, Buffer>>((resolve, reject) => {
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
    if (op.type == 'put') {
      op.key = Buffer.from('changed');
      op.value = Buffer.from('changed');
    }
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

  await db.put(Buffer.from('beep'), Buffer.from('boop'));

  console.log(await db.get(Buffer.from('changed')));

  // await db.del('beep');
  // await db.batch([
  //   { type: 'put', key: 'fatehr', value: 'gloop' },
  //   { type: 'put', key: 'omther', value: 'what' }
  // ]);

  // TEST if you can mutate the operation
  // if you can't you cannot use this to do encryption
  // if you can, then this is precisely what you need to use for an "encryption layer"
  // also how do other level systems work? I guess they wrap the db module as well
  // but these hooks only work against put, del, batch, not get
  // that's dumb, without a get hook, that can mutate things how can you use this as a generic wrapper

}

main();
