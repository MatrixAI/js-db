import type { KeyPath } from '@/types';
import * as utils from '@/utils';

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
});
