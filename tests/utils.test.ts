import type { KeyPath } from '@/types';
import nodeCrypto from 'crypto';
import * as utils from '@/utils';
import * as testUtils from './utils';

describe('utils', () => {
  const keyPaths: Array<KeyPath> = [
    // Normal keys
    ['foo'],
    ['foo', 'bar'],
    // Empty keys are possible
    [''],
    ['', ''],
    ['foo', ''],
    ['foo', '', ''],
    ['', 'foo', ''],
    ['', '', ''],
    ['', '', 'foo'],
    ['foo', '', '', 'foo'],
    // Separator can be used in key part
    ['foo', 'bar', Buffer.concat([utils.sep, Buffer.from('key'), utils.sep])],
    [utils.sep],
    [Buffer.concat([utils.sep, Buffer.from('foobar')])],
    [Buffer.concat([Buffer.from('foobar'), utils.sep])],
    [Buffer.concat([utils.sep, Buffer.from('foobar'), utils.sep])],
    [
      Buffer.concat([
        utils.sep,
        Buffer.from('foobar'),
        utils.sep,
        Buffer.from('foobar'),
      ]),
    ],
    [
      Buffer.concat([
        Buffer.from('foobar'),
        utils.sep,
        Buffer.from('foobar'),
        utils.sep,
      ]),
    ],
    [
      '',
      Buffer.concat([
        Buffer.from('foobar'),
        utils.sep,
        Buffer.from('foobar'),
        utils.sep,
      ]),
      '',
    ],
    // Separator can be used in level parts
    [Buffer.concat([utils.sep, Buffer.from('foobar')]), 'key'],
    [Buffer.concat([Buffer.from('foobar'), utils.sep]), 'key'],
    [Buffer.concat([utils.sep, Buffer.from('foobar'), utils.sep]), 'key'],
    [
      Buffer.concat([
        utils.sep,
        Buffer.from('foobar'),
        utils.sep,
        Buffer.from('foobar'),
      ]),
      'key',
    ],
    [
      Buffer.concat([
        Buffer.from('foobar'),
        utils.sep,
        Buffer.from('foobar'),
        utils.sep,
      ]),
      'key',
    ],
    [
      '',
      Buffer.concat([
        Buffer.from('foobar'),
        utils.sep,
        Buffer.from('foobar'),
        utils.sep,
      ]),
      'key',
      '',
    ],
  ];
  test.each(keyPaths.map((kP) => [kP]))(
    'parse key paths %s',
    (keyPath: KeyPath) => {
      const key = utils.keyPathToKey(keyPath);
      const keyPath_ = utils.parseKey(key);
      expect(keyPath.map((b) => b.toString())).toStrictEqual(
        keyPath_.map((b) => b.toString()),
      );
    },
  );
  test('base128 encoding/decoding', () => {
    // Base 128 alphabet is alphabetical and starts at 0x01 (skips 0x00)
    // it uses the same rfc4648 algorithm that base64 uses
    const table = [
      [[], [0x01]],
      [[0x00], [0x02, 0x02]],
      [[0x01], [0x02, 0x42]],
      [[0x02], [0x03, 0x02]],
      [[0x03], [0x03, 0x42]],
      [[0x04], [0x04, 0x02]],
      [[0x05], [0x04, 0x42]],
      [[0x06], [0x05, 0x02]],
      [[0x07], [0x05, 0x42]],
      [[0x08], [0x06, 0x02]],
      // Larger single bytes
      [[0xec], [0x78, 0x02]],
      [[0xed], [0x78, 0x42]],
      [[0xee], [0x79, 0x02]],
      [[0xef], [0x79, 0x42]],
      [[0xfe], [0x81, 0x02]],
      [[0xff], [0x81, 0x42]],
      // 2 bytes
      [
        [0x00, 0x00],
        [0x02, 0x02, 0x02],
      ],
      [
        [0x00, 0x01],
        [0x02, 0x02, 0x22],
      ],
      [
        [0xfe, 0x00],
        [0x81, 0x02, 0x02],
      ],
      [
        [0xfe, 0x01],
        [0x81, 0x02, 0x22],
      ],
      [
        [0xff, 0x00],
        [0x81, 0x42, 0x02],
      ],
      [
        [0xff, 0x01],
        [0x81, 0x42, 0x22],
      ],
      [
        [0xff, 0xff],
        [0x81, 0x81, 0x62],
      ],
    ];
    for (const [input, output] of table) {
      const inputEncoded = utils.encodePart(Buffer.from(input));
      expect(inputEncoded).toStrictEqual(Buffer.from(output));
      const inputEncodedDecoded = utils.decodePart(inputEncoded);
      expect(inputEncodedDecoded).toStrictEqual(Buffer.from(input));
    }
  });
  test('base128 lexicographic order fuzzing', () => {
    const parts: Array<[number, Buffer]> = Array.from(
      { length: 1000 },
      (_, i) => [i, nodeCrypto.randomBytes(testUtils.getRandomInt(0, 101))],
    );
    const partsEncoded: Array<[number, Buffer]> = parts.map(([i, p]) => [
      i,
      utils.encodePart(p),
    ]);
    parts.sort(([_i, a], [_j, b]) => Buffer.compare(a, b));
    partsEncoded.sort(([_i, a], [_j, b]) => Buffer.compare(a, b));
    expect(parts.map(([i]) => i)).toStrictEqual(partsEncoded.map(([i]) => i));
    for (const [j, [i, pE]] of partsEncoded.entries()) {
      expect([i, utils.decodePart(pE)]).toStrictEqual(parts[j]);
    }
  });
  test('Buffer.compare sorts byte by byte', () => {
    const arr = [
      Buffer.from([0x01]),
      Buffer.from([0x00, 0x00, 0x00]),
      Buffer.from([0x00, 0x00]),
      Buffer.from([]),
    ];
    arr.sort(Buffer.compare);
    expect(arr).toStrictEqual([
      // Therefore empty buffer sorts first
      Buffer.from([]),
      // Therefore `aa` is earlier than `aaa`
      Buffer.from([0x00, 0x00]),
      Buffer.from([0x00, 0x00, 0x00]),
      // Therefore `aa` is earlier than `z`
      Buffer.from([0x01]),
    ]);
  });
});
