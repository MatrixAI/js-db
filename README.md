# js-db

staging: [![pipeline status](https://gitlab.com/MatrixAI/open-source/js-db/badges/staging/pipeline.svg)](https://gitlab.com/MatrixAI/open-source/js-db/commits/staging)
master: [![pipeline status](https://gitlab.com/MatrixAI/open-source/js-db/badges/master/pipeline.svg)](https://gitlab.com/MatrixAI/open-source/js-db/commits/master)

DB is library managing key value state for MatrixAI's JavaScript/TypeScript applications.

This forks classic-level's C++ binding code around LevelDB 1.20. Differences from classic-level:

* Uses TypeScript from ground-up
* Supports Snapshot-Isolation based transactions via `DBTransaction`
* API supports "key paths" which can be used to manipulate "levels" of nested keys
* Value encryption (key-encryption is not supported yet) - requires additional work with block-encryption

## Installation

```sh
npm install --save @matrixai/db
```

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

```sh
# npm login
npm version patch # major/minor/patch
npm run build
npm publish --access public
git push
git push --tags
```
