import type { Callback, KeyPath, LevelPath } from './types';
import * as errors from './errors';

/**
 * Separator is a single null byte
 * This special symbol must not appear in the encoded parts
 */
const sep = Buffer.from([0x00]);

/**
 * Empty parts will be encoded as a single 0x01 byte
 */
const empty = Buffer.from([0x01]);

/**
 * Lexicographically ordered base 128 alphabet
 * The alphabet starts at 0x02 (skipping 0x00 and 0x01)
 */
const alphabet = Buffer.from(
  Array.from({ length: 128 }, (_, i) => {
    return i + 2;
  }),
);

/**
 * Encode level or key part using base 128 encoding
 * Empty parts are encoded with the special empty symbol
 */
function encodePart(part: Buffer): Buffer {
  if (part.byteLength === 0) {
    return empty;
  }
  // Start encoding
  const mask = (1 << 7) - 1;
  const out: Array<number> = [];
  let bits = 0; // Number of bits currently in the buffer
  let buffer = 0; // Bits waiting to be written out, MSB first
  for (let i = 0; i < part.length; ++i) {
    // Slurp data into the buffer
    buffer = (buffer << 8) | part[i];
    bits += 8;
    // Write out as much as we can
    while (bits > 7) {
      bits -= 7;
      out.push(alphabet[mask & (buffer >> bits)]);
    }
  }
  // Partial character
  if (bits) {
    out.push(alphabet[mask & (buffer << (7 - bits))]);
  }
  return Buffer.from(out);
}

/**
 * Decode level or key part from base 128
 * The special empty symbol is decoded as an empty buffer
 */
function decodePart(data: Buffer): Buffer {
  if (data.equals(empty)) {
    return Buffer.allocUnsafe(0);
  }
  const codes: Record<number, number> = {};
  for (let i = 0; i < alphabet.length; ++i) {
    codes[alphabet[i]] = i;
  }
  // Allocate the output
  const out = new Uint8Array(((data.length * 7) / 8) | 0);
  // Parse the data
  let bits = 0; // Number of bits currently in the buffer
  let buffer = 0; // Bits waiting to be written out, MSB first
  let written = 0; // Next byte to write
  for (let i = 0; i < data.length; ++i) {
    // Read one character from the input
    const value = codes[data[i]];
    if (value === undefined) {
      throw new SyntaxError(`Non-Base128 character`);
    }
    // Append the bits to the buffer
    buffer = (buffer << 7) | value;
    bits += 7;
    // Write out some bits if the buffer has a byte's worth
    if (bits >= 8) {
      bits -= 8;
      out[written++] = 0xff & (buffer >> bits);
    }
  }
  return Buffer.from(out);
}

/**
 * Used to convert possible KeyPath into legal KeyPath
 */
function toKeyPath(keyPath: KeyPath | string | Buffer): KeyPath {
  if (!Array.isArray(keyPath)) {
    keyPath = [keyPath] as KeyPath;
  }
  if (keyPath.length < 1) {
    keyPath = [''];
  }
  return keyPath;
}

/**
 * Converts KeyPath to key buffer
 * e.g. ['A', 'B'] => !A!B (where ! is the sep)
 * An empty key path is converted to `['']`
 * Level parts is allowed to contain the separator
 * Key actual part is allowed to contain the separator
 */
function keyPathToKey(keyPath: KeyPath): Buffer {
  if (keyPath.length < 1) {
    keyPath = [''];
  }
  const keyPart = keyPath.slice(-1)[0];
  const levelPath = keyPath.slice(0, -1);
  return Buffer.concat([
    levelPathToKey(levelPath),
    encodePart(
      typeof keyPart === 'string' ? Buffer.from(keyPart, 'utf-8') : keyPart,
    ),
  ]);
}

/**
 * Converts LevelPath to key buffer
 * e.g. ['A', 'B'] => !A!!B! (where ! is the sep)
 * Level parts are allowed to contain the separator before encoding
 */
function levelPathToKey(levelPath: LevelPath): Buffer {
  return Buffer.concat(
    levelPath.map((p) => {
      p = typeof p === 'string' ? Buffer.from(p, 'utf-8') : p;
      p = encodePart(p);
      return Buffer.concat([sep, p, sep]);
    }),
  );
}

/**
 * Converts key buffer back into KeyPath
 * e.g. !A!!B!C => ['A', 'B', 'C'] (where ! is the sep)
 * Returned parts are always buffers
 *
 * BNF grammar of key buffer:
 *   path => levels:ls keyActual:k -> [...ls, k] | keyActual:k -> [k]
 *   levels => level:l levels:ls -> [l, ...ls] | '' -> []
 *   level => sep .*?:l sep -> l
 *   sep => 0x00
 *   keyActual => .*:k -> [k]
 */
function parseKey(key: Buffer): KeyPath {
  const [bufs] = parsePath(key);
  if (bufs.length < 1) {
    throw new TypeError('Buffer is not a key');
  }
  for (let i = 0; i < bufs.length; i++) {
    bufs[i] = decodePart(bufs[i]);
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
    } else {
      levelBytes.push(b);
    }
  }
  if (sepEnd == null) {
    throw new errors.ErrorDBParseKey('Missing separator end');
  }
  const level = Buffer.from(levelBytes);
  const remaining = input.subarray(sepEnd + 1);
  return [[level], remaining];
}

function parseKeyActual(input: Buffer): [Array<Buffer>, Buffer] {
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

/**
 * Convert callback-style to promise-style
 * If this is applied to overloaded function
 * it will only choose one of the function signatures to use
 */
function promisify<
  T extends Array<unknown>,
  P extends Array<unknown>,
  R extends T extends [] ? void : T extends [unknown] ? T[0] : T,
>(
  f: (...args: [...params: P, callback: Callback<T>]) => unknown,
): (...params: P) => Promise<R> {
  // Uses a regular function so that `this` can be bound
  const g = function (...params: P): Promise<R> {
    return new Promise((resolve, reject) => {
      const callback = (error, ...values) => {
        if (error != null) {
          return reject(error);
        }
        if (values.length === 0) {
          (resolve as () => void)();
        } else if (values.length === 1) {
          resolve(values[0] as R);
        } else {
          resolve(values as R);
        }
        return;
      };
      params.push(callback);
      f.apply(this, params);
    });
  };
  Object.defineProperty(g, 'name', { value: f.name });
  return g;
}

/**
 * Native addons expect strict optional properties
 * Properties that have the value undefined may be misinterpreted
 * Apply these to options objects before passing them to the native addon
 */
function filterUndefined(o: object): void {
  Object.keys(o).forEach((k) => {
    if (o[k] === undefined) {
      delete o[k];
    }
  });
}

export {
  sep,
  encodePart,
  decodePart,
  toKeyPath,
  keyPathToKey,
  levelPathToKey,
  parseKey,
  sepExists,
  serialize,
  deserialize,
  toArrayBuffer,
  fromArrayBuffer,
  promisify,
  filterUndefined,
};
