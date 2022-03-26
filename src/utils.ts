import type { NonEmptyArray, KeyPath, LevelPath } from './types';
import * as errors from './errors';

/**
 * Separator is a single null byte
 * During iteration acquiring a sublevel requires iterating
 * between 0x00 and 0x01
 */
const sep = Buffer.from([0]);

/**
 * Converts KeyPath to key buffer
 * e.g. ['A', 'B'] => !A!B (where ! is the sep)
 * The key path must not be empty
 * Level parts must not contain the separator
 * Key actual part is allowed to contain the separator
 */
function keyPathToKey(keyPath: KeyPath): Buffer {
  const keyPart = keyPath.slice(-1)[0];
  const levelPath = keyPath.slice(0, -1);
  return Buffer.concat([
    levelPathToKey(levelPath),
    typeof keyPart === 'string' ? Buffer.from(keyPart, 'utf-8') : keyPart,
  ]);
}

/**
 * Converts LevelPath to key buffer
 * e.g. ['A', 'B'] => !A!!B! (where ! is the sep)
 * Level parts must not contain the separator
 */
function levelPathToKey(levelPath: LevelPath): Buffer {
  return Buffer.concat(
    levelPath.map((p) =>
      Buffer.concat([
        sep,
        typeof p === 'string' ? Buffer.from(p, 'utf-8') : p,
        sep,
      ]),
    ),
  );
}

/**
 * Converts key buffer back into KeyPath
 * e.g. !A!!B!C => ['A', 'B', 'C'] (where ! is the sep)
 * Returned parts are always buffers
 *
 * BNF grammar of key buffer:
 *   path => levels:ls keyActual:k -> [...ls, k] | keyActual
 *   levels => level:l levels:ls finalKey -> [l, ...ls] | '' -> []
 *   level => sep [^sep]+:l sep -> l
 *   sep => '!'
 *   keyActual => .+
 */
function parseKey(key: Buffer): KeyPath {
  const [bufs] = parsePath(key);
  if (!isNonEmptyArray(bufs)) {
    throw new TypeError('Buffer is not a key');
  }
  return bufs;
}

function parsePath(input: Buffer): [Array<Buffer>, Buffer] {
  try {
    let output: Array<Buffer> = [];
    let input_: Buffer = input;
    let output_: Array<Buffer>;
    [output_, input_] = parseLevels(input_);
    output = output.concat(output_);
    [output_, input_] = parseKeyActual(input_);
    output = output.concat(output_);
    return [output, input_];
  } catch (e) {
    let output: Array<Buffer> = [];
    let input_: Buffer = input;
    let output_: Array<Buffer>;
    // eslint-disable-next-line prefer-const
    [output_, input_] = parseKeyActual(input_);
    output = output.concat(output_);
    return [output, input_];
  }
}

function parseLevels(
  input: Buffer,
): [output: Array<Buffer>, remaining: Buffer] {
  let output: Array<Buffer> = [];
  try {
    let input_: Buffer = input;
    let output_: Array<Buffer>;
    [output_, input_] = parseLevel(input_);
    output = output.concat(output_);
    [output_, input_] = parseLevels(input_);
    output = output.concat(output_);
    parseKeyActual(input_);
    return [output, input_];
  } catch (e) {
    return [[], input];
  }
}

function parseLevel(input: Buffer): [Array<Buffer>, Buffer] {
  const sepStart = input.indexOf(sep);
  if (sepStart === -1) {
    throw new errors.ErrorDBParseKey('Missing separator start');
  }
  const sepEnd = input.indexOf(sep, sepStart + 1);
  if (sepEnd === -1) {
    throw new errors.ErrorDBParseKey('Missing separator end');
  }
  const level = input.subarray(sepStart + 1, sepEnd);
  const remaining = input.subarray(sepEnd + 1);

  return [[level], remaining];
}

function parseKeyActual(input: Buffer): [Array<Buffer>, Buffer] {
  if (input.byteLength < 1) {
    throw new errors.ErrorDBParseKey('Key cannot be empty');
  }
  return [[input], input.subarray(input.byteLength)];
}

/**
 * Checks if the KeyPath contains the separator
 * This only checks the LevelPath part
 */
function checkSepKeyPath(keyPath: KeyPath): boolean {
  const levelPath = keyPath.slice(0, -1);
  return checkSepLevelPath(levelPath);
}

/**
 * Checks if LevelPath contains the separator
 */
function checkSepLevelPath(levelPath: LevelPath): boolean {
  return levelPath.some(sepExists);
}

/**
 * Checks if the separator exists in a string or buffer
 * This only needs to applied to the LevelPath, not the final key
 */
function sepExists(data: string | Buffer): boolean {
  if (typeof data === 'string') {
    return data.includes(sep.toString('utf-8'));
  } else {
    return data.includes(sep);
  }
}

function serialize<T>(value: T): Buffer {
  return Buffer.from(JSON.stringify(value), 'utf-8');
}

function deserialize<T>(value_: Buffer): T {
  try {
    return JSON.parse(value_.toString('utf-8'));
  } catch (e) {
    if (e instanceof SyntaxError) {
      throw new errors.ErrorDBParseValue();
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
 * Type guard for NonEmptyArray
 */
function isNonEmptyArray<T>(arr: T[]): arr is NonEmptyArray<T> {
  return arr.length > 0;
}

export {
  sep,
  keyPathToKey,
  levelPathToKey,
  parseKey,
  checkSepKeyPath,
  checkSepLevelPath,
  sepExists,
  isNonEmptyArray,
  serialize,
  deserialize,
  toArrayBuffer,
  fromArrayBuffer,
};
