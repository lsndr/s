import { describe, expect, it } from 'vitest';
import { parseServer } from './server.js';

const ports = { 'amqp:': 5672, 'amqps:': 5671 };

describe('parseServer', () => {
  it('takes the host and the port of the url', () => {
    // act
    const server = parseServer('amqp://guest:secret@broker:5673/vhost', ports);

    // assert
    expect(server).toEqual({ address: 'broker', port: 5673 });
  });

  it('falls back to the default port of the protocol', () => {
    // act
    const server = parseServer('amqps://broker', ports);

    // assert
    expect(server).toEqual({ address: 'broker', port: 5671 });
  });

  it('leaves out the port when there is no default for the protocol', () => {
    // act
    const server = parseServer('other://broker', ports);

    // assert
    expect(server).toEqual({ address: 'broker' });
  });

  it('returns nothing for a url that cannot be parsed', () => {
    // act
    const server = parseServer('not a url', ports);

    // assert
    expect(server).toBeUndefined();
  });
});
