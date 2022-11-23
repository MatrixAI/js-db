# js-db

staging: [![pipeline status](https://gitlab.com/MatrixAI/open-source/js-db/badges/staging/pipeline.svg)](https://gitlab.com/MatrixAI/open-source/js-db/commits/staging)
master: [![pipeline status](https://gitlab.com/MatrixAI/open-source/js-db/badges/master/pipeline.svg)](https://gitlab.com/MatrixAI/open-source/js-db/commits/master)

DB is library managing key value state for MatrixAI's JavaScript/TypeScript applications.

This forks classic-level's C++ binding code around LevelDB 1.20. Differences from classic-level:

* Uses TypeScript from ground-up
* Supports Snapshot-Isolation based transactions via `DBTransaction`
* API supports "key paths" which can be used to manipulate "levels" of nested keys
* Value encryption (key-encryption is not supported yet) - requires additional work with block-encryption
* Uses RocksDB

## Installation

```sh
npm install --save @matrixai/db
```

## Usage

```ts
import { DB } from '@matrixai/db';

async function main () {

  const key = Buffer.from([
    0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x02, 0x03,
    0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x02, 0x03,
  ]);

  const encrypt = async (
    key: ArrayBuffer,
    plainText: ArrayBuffer
  ): Promise<ArrayBuffer> {
    return plainText;
  };

  const decrypt = async (
    key: ArrayBuffer,
    cipherText: ArrayBuffer
  ): Promise<ArrayBuffer | undefined> {
    return cipherText;
  }

  const db = await DB.createDB({
    dbPath: './tmp/db',
    crypto: {
      key,
      ops: { encrypt, decrypt },
    },
    fresh: true,
  });

  await db.put(['level', Buffer.from([0x30, 0x30]), 'a'], 'value');
  await db.put(['level', Buffer.from([0x30, 0x31]), 'b'], 'value');
  await db.put(['level', Buffer.from([0x30, 0x32]), 'c'], 'value');
  await db.put(['level', Buffer.from([0x30, 0x33]), 'c'], 'value');

  console.log(await db.get(['level', Buffer.from([0x30, 0x32]), 'c']));

  await db.del(['level', Buffer.from([0x30, 0x32]), 'c']);

  for await (const [kP, v] of db.iterator({
    lt: [Buffer.from([0x30, 0x32]), ''],
  }, ['level'])) {
    console.log(kP, v);
  }

  await db.stop();
}

main();
```

If you created the `DB` with a `crypto` object, then upon restarting the `DB`, you must pass in the same `crypto` object.

## Development

Run `nix-shell`, and once you're inside, you can use:

```sh
# install (or reinstall packages from package.json)
npm install
# build the dist
npm run build
# run the repl (this allows you to import from ./src)
npm run ts-node
# run the tests
npm run test
# lint the source code
npm run lint
# automatically fix the source
npm run lintfix
```

## Benchmarks

```sh
npm run bench
```

View benchmarks here: https://github.com/MatrixAI/js-db/blob/master/benches/results with https://raw.githack.com/

### Docs Generation

```sh
npm run docs
```

See the docs at: https://matrixai.github.io/js-db/

### Publishing

Publishing is handled automatically by the staging pipeline.

Prerelease:

```sh
# npm login
npm version prepatch --preid alpha # premajor/preminor/prepatch
git push --follow-tags
```

Release:

```sh
# npm login
npm version patch # major/minor/patch
git push --follow-tags
```

Manually:

```sh
# npm login
npm version patch # major/minor/patch
npm run build
npm publish --access public
git push
git push --tags
```
