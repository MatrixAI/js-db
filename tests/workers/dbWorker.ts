import type { DBWorkerModule } from './dbWorkerModule.js';
import { expose } from 'threads/worker';
import dbWorker from './dbWorkerModule.js';

expose(dbWorker);

export type { DBWorkerModule };
