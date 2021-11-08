// try and get a proper index going

import level from 'level';
import AutoIndex from 'level-auto-index';
import subleveldown from 'subleveldown';

async function main () {

  const db = await new Promise((resolve, reject) => {
    const db = level(
      './tmp/db',
      {
        keyEncoding: 'binary',
        valueEncoding: 'binary'
      },
      (e) => {
        if (e) {
          reject(e);
        } else {
          resolve(db);
        }
      }
    );
  });

  // this is the level
  // but it can also be the root right?
  const dbLevel = await new Promise((resolve) => {
    const dbLevelNew = subleveldown(
      db,
      'vaults',
      {
        keyEncoding: 'binary',
        valueEncoding: 'binary',
        open: (cb) => {
          cb(undefined);
          resolve(dbLevelNew);
        }
      }
    );
  });

  // the level-idx uses a single
  // index db, instead of auto index
  // which uses multiple sublevels
  // but this can be a problem sort of
  // basically the keys and value encoding of the index db
  // has to be buffers
  // cause our keys can string or Buffer
  // but i reckon it makes sense to store the index keys always as string
  // cause even if we get a string
  // the key is "utf-8" encoded
  // thus we always have binary keys





}

main();
