import type {
  DBDomain,
  ResourceAcquire,
  ResourceRelease,
  Resources,
} from './types';

import sublevelprefixer from 'sublevel-prefixer';
import * as dbErrors from './errors';

const prefix = '!';

const prefixer = sublevelprefixer(prefix);

function domainPath(levels: DBDomain, key: string | Buffer): string | Buffer {
  if (!levels.length) {
    return key;
  }
  let prefix = key;
  for (let i = levels.length - 1; i >= 0; i--) {
    prefix = prefixer(levels[i], prefix);
  }
  return prefix;
}

function serialize<T>(value: T): Buffer {
  return Buffer.from(JSON.stringify(value), 'utf-8');
}

function deserialize<T>(value_: Buffer): T {
  try {
    return JSON.parse(value_.toString('utf-8'));
  } catch (e) {
    if (e instanceof SyntaxError) {
      throw new dbErrors.ErrorDBParse();
    }
    throw e;
  }
}

/**
 * Slice-copies the Node Buffer to a new ArrayBuffer
 */
function toArrayBuffer(b: Buffer): ArrayBuffer {
  return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength);
}

/**
 * Wraps ArrayBuffer in Node Buffer with zero copy
 */
function fromArrayBuffer(
  b: ArrayBuffer,
  offset?: number,
  length?: number,
): Buffer {
  return Buffer.from(b, offset, length);
}

/**
 * Make sure to explicitly declare or cast `acquires` as a tuple using `[ResourceAcquire...]` or `as const`
 */
async function withF<
  ResourceAcquires extends
    | readonly [ResourceAcquire<unknown>]
    | readonly ResourceAcquire<unknown>[],
  T,
>(
  acquires: ResourceAcquires,
  f: (resources: Resources<ResourceAcquires>) => Promise<T>,
): Promise<T> {
  const releases: Array<ResourceRelease> = [];
  const resources: Array<unknown> = [];
  let e_: Error | undefined;
  try {
    for (const acquire of acquires) {
      const [release, resource] = await acquire();
      releases.push(release);
      resources.push(resource);
    }
    return await f(resources as unknown as Resources<ResourceAcquires>);
  } catch (e) {
    e_ = e;
    throw e;
  } finally {
    releases.reverse();
    for (const release of releases) {
      await release(e_);
    }
  }
}

/**
 * Make sure to explicitly declare or cast `acquires` as a tuple using `[ResourceAcquire...]` or `as const`
 */
async function* withG<
  ResourceAcquires extends
    | readonly [ResourceAcquire<unknown>]
    | readonly ResourceAcquire<unknown>[],
  T = unknown,
  TReturn = any,
  TNext = unknown,
>(
  acquires: ResourceAcquires,
  g: (
    resources: Resources<ResourceAcquires>,
  ) => AsyncGenerator<T, TReturn, TNext>,
): AsyncGenerator<T, TReturn, TNext> {
  const releases: Array<ResourceRelease> = [];
  const resources: Array<unknown> = [];
  let e_: Error | undefined;
  try {
    for (const acquire of acquires) {
      const [release, resource] = await acquire();
      releases.push(release);
      resources.push(resource);
    }
    return yield* g(resources as unknown as Resources<ResourceAcquires>);
  } catch (e) {
    e_ = e;
    throw e;
  } finally {
    releases.reverse();
    for (const release of releases) {
      await release(e_);
    }
  }
}

export {
  prefix,
  domainPath,
  serialize,
  deserialize,
  toArrayBuffer,
  fromArrayBuffer,
  withF,
  withG,
};
