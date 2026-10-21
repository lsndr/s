import { type Dispatcher } from './dispatcher.js';
import { type Subscriber } from './subscriber.js';
import { type Health } from './health.js';

export interface Transport extends Dispatcher, Subscriber, Health {}

export const Transport = Symbol('Transport');
