import type { POJO } from './types';

import { CustomError } from 'ts-custom-error';

type ErrorChain = Error & { cause?: ErrorChain };

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

class ErrorDBLevelSep extends ErrorDB {}

class ErrorDBDecrypt extends ErrorDB {}

class ErrorDBParseKey extends ErrorDB {}

class ErrorDBParseValue extends ErrorDB {}

class ErrorDBTransactionDestroyed extends ErrorDB {}

class ErrorDBTransactionCommitted extends ErrorDB {}

class ErrorDBTransactionNotCommited extends ErrorDB {}

class ErrorDBTransactionRollbacked extends ErrorDB {}

export {
  ErrorDB,
  ErrorDBRunning,
  ErrorDBNotRunning,
  ErrorDBDestroyed,
  ErrorDBCreate,
  ErrorDBDelete,
  ErrorDBLevelSep,
  ErrorDBDecrypt,
  ErrorDBParseKey,
  ErrorDBParseValue,
  ErrorDBTransactionDestroyed,
  ErrorDBTransactionCommitted,
  ErrorDBTransactionNotCommited,
  ErrorDBTransactionRollbacked,
};
