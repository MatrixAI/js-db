import type { Codec } from 'multiformats/bases/base';

import crypto from 'crypto';
import { bases } from 'multiformats/basics';

function randomBytes(size: number): Uint8Array {
  return crypto.randomBytes(size);
}

type MultibaseFormats = keyof typeof bases;

const basesByPrefix: Record<string, Codec<string, string>> = {};
for (const k in bases) {
  const codec = bases[k];
  basesByPrefix[codec.prefix] = codec;
}

function toMultibase(id: Uint8Array, format: MultibaseFormats): string {
  const codec = bases[format];
  return codec.encode(id);
}

function fromMultibase(idString: string): Uint8Array | undefined {
  const prefix = idString[0];
  const codec = basesByPrefix[prefix];
  if (codec == null) {
    return;
  }
  const buffer = codec.decode(idString);
  return buffer;
}

const originalList: Array<Uint8Array> = [];

const total = 100000;

let count = total;
while (count) {
  originalList.push(randomBytes(16));
  count--;
}

originalList.sort(Buffer.compare);
const encodedList = originalList.map(
  (bs) => toMultibase(bs, 'base64')
);

console.log(encodedList);

// const encodedList_ = encodedList.slice();
// encodedList_.sort();

// // encodedList is the same order as originalList
// // if base58btc preserves lexicographic-order
// // then encodedList_ would be the same order

// const l = encodedList[0].length;
// console.log(l);
// for (let i = 0; i < total; i++) {

//   if (encodedList[i].length !== l) {
//     console.log('new length', encodedList[i].length);
//   }

//   if (encodedList[i] !== encodedList_[i]) {
//     console.log('Does not match on:', i);
//     console.log('original order', encodedList[i]);
//     console.log('encoded order', encodedList_[i]);
//     break;
//   }
// }

// const decodedList = encodedList.map(fromMultibase);
// for (let i = 0; i < total; i++) {
//   // @ts-ignore
//   if (!originalList[i].equals(Buffer.from(decodedList[i]))) {
//     console.log('bug in the code');
//     break;
//   }
// }


// // 36 characters became 59 characters

// console.log(encodedList);

// // need to watch the integration testing more
// // so that concurrent testing works better
// // establish promise conventions
// // and then get on the code properly
