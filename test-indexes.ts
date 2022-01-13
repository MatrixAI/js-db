// how does multi-value indexing work normally?
// let's see if it works in existing system
// our 2 choices is move encryption to a lower level, and reuse the same indexing system
// or build indexing on top

import type { LevelUp } from 'levelup';
import type { LevelDB } from 'level';
import type { AbstractLevelDOWN, AbstractIterator, AbstractBatch, AbstractOptions } from 'abstract-leveldown';

import level from 'level';
import hookdown from 'level-hookdown';
import subleveldown from 'subleveldown';
import AutoIndex from 'level-auto-index';
import Index from 'level-idx';
import sublevelprefixer from 'sublevel-prefixer';

async function main () {

  const db = await new Promise<LevelDB<any, any>>((resolve, reject) => {
    const db = level(
      './tmp/db',
      {
        keyEncoding: 'binary',
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

  const posts = await new Promise<LevelUp>((resolve) => {
    const dbLevelNew = subleveldown(
      db,
      'posts',
      {
        keyEncoding: 'binary',
        valueEncoding: 'json',
        open: (cb) => {
          cb(undefined);
          resolve(dbLevelNew);
        }
      }
    );
  });

  const idx = await new Promise<LevelUp>((resolve) => {
    const dbLevelNew = subleveldown(
      db,
      'idx',
      {
        keyEncoding: 'binary',
        open: (cb) => {
          cb(undefined);
          resolve(dbLevelNew);
        }
      }
    );
  });

  // ok if we need to base encode all the things

  // right now the indexes don't work
  // cause it tries to acquire the value straight away
  // the only way would be do a compound index, where  you get 1!a title 2!a title
  // but that doesn't seem to allow us to know how many there is
  // so it has to be a sublevel
  // a title is a sublevel -> pointing to indices
  // but we will need to base encode to avoid the problem of using values there
  // hex encoding is always lexicographic preserving

  // if you're hashing you won't need to base encode at the end
  // and therefore the sort order doesn't matter
  // IdDeterministic can be used for that, as it is just hashing
  // but if you are doing a base encoding
  // then it would just be hex encoding

  const another = await new Promise<LevelUp>((resolve) => {
    const dbLevelNew = subleveldown(
      db,
      'Ti!!tle',
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

  // if we were to create sublevels for each kind of key?
  // if the sublevels had names like this


  // // each Name here is a another sublevel under idx
  // // the accessors tell us how to construct the index
  // // compound indexes are just joined with `!`
  // // it only works with uniqueness in this case
  // Index(posts, idx)
  //   .by('Ti!!tle', 'title')
  //   .by('Length', ['body.length', 'title'])
  //   .by('Author', ['author', 'title']);

  // const post = {
  //   title: 'my title',
  //   body: 'lorem ipsum',
  //   author: 'julian'
  // };


  // // the only thing is to not use `!!` in our sublevel prefixes

  // await posts.put(Buffer.from('some !key'), post);


  // // @ts-ignore
  // posts['byTi!!tle'].get(Buffer.from('my title'), (e, o) => {
  //   console.log('GOT IT', o);
  // });

  // // console.log(await db.get('!idx!!Title!my title'));

  // console.log('STARTING STREAM');
  // const s = db.createReadStream();
  // for await (const o of s) {
  //   // @ts-ignore
  //   console.log('KEY', o.key.toString(), o.value);
  //   // @ts-ignore
  //   console.log('VALUE', o.value);
  // }


  // // @ts-ignore
  // console.log('PROMISE', await posts.byTitle.get(Buffer.from('my title')));
  // console.log(await db.get('!posts!some key'));

  // there a thinking that `!` would never be used for our keys?

}

main();

// seems like level-idx doesn't use the same version of subleveldown as we do
// this means it has an older subleveldown that doesn't seem the throw the error
// "subleveldown": "^4.1.0" doesn't appear to throw that error how funny
// we are using "subleveldown": "^5.0.1", which does throw that errorte
