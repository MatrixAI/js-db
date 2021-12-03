import type { POJO } from './types';

import { CustomError } from 'ts-custom-error';

type ErrorChain = Error & { chain?: ErrorChain };

class ErrorDB extends CustomError {
  data: POJO;
  cause?: ErrorChain;
  constructor(message: string = '', data: POJO = {}, cause?: ErrorChain) {
    super(message);
    this.data = data;
    this.cause = cause;
  }
}

class ErrorDBRunning extends ErrorDB {}

class ErrorDBNotRunning extends ErrorDB {}

class ErrorDBDestroyed extends ErrorDB {}

class ErrorDBCreate extends ErrorDB {}

class ErrorDBDelete extends ErrorDB {}

class ErrorDBLevelPrefix extends ErrorDB {}

class ErrorDBDecrypt extends ErrorDB {}

class ErrorDBParse extends ErrorDB {}

class ErrorDBCommitted extends ErrorDB {}

class ErrorDBNotCommited extends ErrorDB {}

class ErrorDBRollbacked extends ErrorDB {}

export {
  ErrorDB,
  ErrorDBRunning,
  ErrorDBNotRunning,
  ErrorDBDestroyed,
  ErrorDBCreate,
  ErrorDBDelete,
  ErrorDBLevelPrefix,
  ErrorDBDecrypt,
  ErrorDBParse,
  ErrorDBCommitted,
  ErrorDBNotCommited,
  ErrorDBRollbacked,
};
