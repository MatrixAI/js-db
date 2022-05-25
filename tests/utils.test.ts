import type { KeyPath } from '@/types';
import * as utils from '@/utils';

describe('utils', () => {
  test('parse key paths', () => {
    const keyPaths: Array<KeyPath> = [
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
      // Escape can be used in key part
      [utils.esc],
      [Buffer.concat([utils.esc, Buffer.from('foobar')])],
      [Buffer.concat([Buffer.from('foobar'), utils.esc])],
      [Buffer.concat([utils.esc, Buffer.from('foobar'), utils.esc])],
      [
        Buffer.concat([
          utils.esc,
          Buffer.from('foobar'),
          utils.esc,
          Buffer.from('foobar'),
        ]),
      ],
      [
        Buffer.concat([
          Buffer.from('foobar'),
          utils.esc,
          Buffer.from('foobar'),
          utils.esc,
        ]),
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
      // Escape can be used in level parts
      [Buffer.concat([utils.sep, utils.esc, utils.sep]), 'key'],
      [Buffer.concat([utils.esc, utils.esc, utils.esc]), 'key'],
    ];
    for (const keyPath of keyPaths) {
      const key = utils.keyPathToKey(keyPath);
      const keyPath_ = utils.parseKey(key);
      expect(keyPath.map((b) => b.toString())).toStrictEqual(
        keyPath_.map((b) => b.toString()),
      );
    }
  });
});
