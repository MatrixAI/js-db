import type { KeyPath, LevelPath } from './types';
import * as errors from './errors';

/**
 * Separator is a single null byte
 * During iteration acquiring a sublevel requires iterating
 * between 0x00 and 0x01
 */
const sep = Buffer.from([0]);

/**
 * Escape is a single `\` byte
 * This is used to escape the separator and literal `\`
 */
const esc = Buffer.from([92]);

/**
 * Converts KeyPath to key buffer
 * e.g. ['A', 'B'] => !A!B (where ! is the sep)
 * An empty key path is converted to `['']`
 * Level and key parts must not contain the separator
 */
function keyPathToKey(keyPath: KeyPath): Buffer {
  if (keyPath.length < 1) {
    keyPath = [''];
  }
  const keyPart = keyPath.slice(-1)[0];
  const levelPath = keyPath.slice(0, -1);
  return Buffer.concat([
    levelPathToKey(levelPath),
    escapePart(
      typeof keyPart === 'string' ? Buffer.from(keyPart, 'utf-8') : keyPart,
    ),
  ]);
}

/**
 * Converts LevelPath to key buffer
 * e.g. ['A', 'B'] => !A!!B! (where ! is the sep)
 * Level parts must not contain the separator
 */
function levelPathToKey(levelPath: LevelPath): Buffer {
  return Buffer.concat(
    levelPath.map((p) => {
      p = typeof p === 'string' ? Buffer.from(p, 'utf-8') : p;
      p = escapePart(p);
      return Buffer.concat([sep, p, sep]);
    }),
  );
}

/**
 * Escapes level and key parts for escape and separator
 */
function escapePart(buf: Buffer): Buffer {
  const bytes: Array<number> = [];
  for (let i = 0; i < buf.byteLength; i++) {
    const b = buf[i];
    if (b === esc[0]) {
      bytes.push(esc[0], b);
    } else if (b === sep[0]) {
      bytes.push(esc[0], b);
    } else {
      bytes.push(b);
    }
  }
  return Buffer.from(bytes);
}

/**
 * Unescapes level and key parts of escape and separator
 */
function unescapePart(buf: Buffer): Buffer {
  const bytes: Array<number> = [];
  for (let i = 0; i < buf.byteLength; i++) {
    const b = buf[i];
    if (b === esc[0]) {
      const n = buf[i + 1];
      if (n === esc[0]) {
        bytes.push(n);
      } else if (n === sep[0]) {
        bytes.push(n);
      } else {
        throw new SyntaxError('Invalid escape sequence');
      }
      i++;
    } else {
      bytes.push(b);
    }
  }
  return Buffer.from(bytes);
}

/**
 * Converts key buffer back into KeyPath
 * e.g. !A!!B!C => ['A', 'B', 'C'] (where ! is the sep)
 * Returned parts are always buffers
 *
 * BNF grammar of key buffer:
 *   path => levels:ls keyActual:k -> [...ls, k] | keyActual:k -> [k]
 *   levels => level:l levels:ls -> [l, ...ls] | '' -> []
 *   level => sep .+?:l (?<!escape) sep (?>.+) -> l
 *   sep => 0x00
 *   escape => 0x5c
 *   keyActual => .+:k -> [k]
 */
function parseKey(key: Buffer): KeyPath {
  const [bufs] = parsePath(key);
  if (bufs.length < 1) {
    throw new TypeError('Buffer is not a key');
  }
  for (let i = 0; i < bufs.length; i++) {
    bufs[i] = unescapePart(bufs[i]);
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
  let sepEnd: number | undefined;
  const levelBytes: Array<number> = [];
  const buf = input.subarray(sepStart + 1);
  for (let i = 0; i < buf.byteLength; i++) {
    const b = buf[i];
    if (b === sep[0]) {
      // Note that `buf` is a subarray offset from the input
      // therefore the `sepEnd` must be offset by the same length
      sepEnd = i + (sepStart + 1);
      break;
    } else if (b === esc[0]) {
      const n = buf[i + 1];
      // Even if undefined
      if (n !== esc[0] && n !== sep[0]) {
        throw new errors.ErrorDBParseKey('Invalid escape sequence');
      }
      // Push the n
      levelBytes.push(b, n);
      // Skip the n
      i++;
    } else {
      levelBytes.push(b);
    }
  }
  if (sepEnd == null) {
    throw new errors.ErrorDBParseKey('Missing separator end');
  }
  if (levelBytes.length < 1) {
    throw new errors.ErrorDBParseKey('Level cannot be empty');
  }
  const level = Buffer.from(levelBytes);
  const remaining = input.subarray(sepEnd + 1);
  if (remaining.byteLength < 1) {
    throw new errors.ErrorDBParseKey('Level cannot be followed by empty');
  }
  return [[level], remaining];
}

function parseKeyActual(input: Buffer): [Array<Buffer>, Buffer] {
  if (input.byteLength < 1) {
    throw new errors.ErrorDBParseKey('Key cannot be empty');
  }
  return [[input], input.subarray(input.byteLength)];
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
      throw new errors.ErrorDBParseValue(e.message, { cause: e });
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

export {
  sep,
  esc,
  escapePart,
  unescapePart,
  keyPathToKey,
  levelPathToKey,
  parseKey,
  sepExists,
  serialize,
  deserialize,
  toArrayBuffer,
  fromArrayBuffer,
};
