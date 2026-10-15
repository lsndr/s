import { afterEach, describe, expect, it, vi } from 'vitest';
import { setImmediate as flush } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type Clients } from '../clients.js';
import { Handler } from '../handler.js';
import { Streams } from '../streams.js';
import { UnblockLoop } from './unblock.js';

class TestEvent {}

const READ_CLIENT_ID = 42;

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });

  return { promise, resolve };
}

describe('UnblockLoop', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function setup({
    slotsPerStream = 1,
    streamNames = ['a'],
    readClientKnown = true,
  } = {}) {
    const streams = new Streams(slotsPerStream);
    const clientUnblock = vi.fn((..._args: [number, string]) =>
      Promise.resolve(1),
    );
    const clients = {
      id: vi.fn(() => (readClientKnown ? READ_CLIENT_ID : undefined)),
      get: vi.fn(() => Promise.resolve({ clientUnblock })),
    };
    const controller = new AbortController();
    const loop = new UnblockLoop(
      clients as unknown as Clients<'read' | 'command'>,
      streams,
    );
    const register = (name: string) =>
      streams.register(name, TestEvent, new Handler('handler', vi.fn()));

    streamNames.forEach(register);

    return {
      clients,
      clientUnblock,
      controller,
      register,
      next: () => loop.next(controller.signal),
      /** Occupies a slot of a stream until the returned function is called. */
      hold: (name: string, id: string) => {
        const held = deferred();

        streams.run(name, id, () => held.promise);

        return held.resolve;
      },
    };
  }

  it('unblocks the read client for the streams that are free at the start', async () => {
    const { next, clientUnblock, clients } = setup();

    await next();

    expect(clients.get).toHaveBeenCalledWith('command');
    expect(clientUnblock).toHaveBeenCalledExactlyOnceWith(
      READ_CLIENT_ID,
      'TIMEOUT',
    );
  });

  it('waits for a change when nothing is new', async () => {
    const { next, clientUnblock } = setup();
    await next();
    clientUnblock.mockClear();
    const onNext = vi.fn();

    void next().then(onNext);
    await flush();

    expect(onNext).not.toHaveBeenCalled();
    expect(clientUnblock).not.toHaveBeenCalled();
  });

  it('unblocks when a slot of a full stream is released', async () => {
    const { next, hold, clientUnblock } = setup({ streamNames: ['a', 'b'] });
    const release = hold('b', '1');
    await next();
    const waiting = next();
    clientUnblock.mockClear();

    release();
    await waiting;
    await next();

    expect(clientUnblock).toHaveBeenCalledExactlyOnceWith(
      READ_CLIENT_ID,
      'TIMEOUT',
    );
  });

  it('unblocks when a stream is registered', async () => {
    const { next, register, clientUnblock } = setup();
    await next();
    const waiting = next();
    clientUnblock.mockClear();

    register('b');
    await waiting;
    await next();

    expect(clientUnblock).toHaveBeenCalledExactlyOnceWith(
      READ_CLIENT_ID,
      'TIMEOUT',
    );
  });

  it('does not unblock when a stream stays readable', async () => {
    const { next, hold, clientUnblock } = setup({ slotsPerStream: 2 });
    const release = hold('a', '1');
    await next();
    const waiting = next();
    clientUnblock.mockClear();

    release();
    await waiting;
    void next();
    await flush();

    expect(clientUnblock).not.toHaveBeenCalled();
  });

  it('does not unblock when a stream becomes full', async () => {
    const { next, hold, clientUnblock } = setup({ streamNames: ['a', 'b'] });
    await next();
    const waiting = next();
    clientUnblock.mockClear();

    hold('b', '1');
    await waiting;
    void next();
    await flush();

    expect(clientUnblock).not.toHaveBeenCalled();
  });

  it('warns and skips when the id of the read client is unknown', async () => {
    const warn = vi
      .spyOn(Logger.prototype, 'warn')
      .mockImplementation(() => undefined);
    const { next, clientUnblock } = setup({ readClientKnown: false });

    await next();

    expect(warn).toHaveBeenCalledOnce();
    expect(clientUnblock).not.toHaveBeenCalled();
  });

  it('stops waiting when the signal is aborted', async () => {
    const { next, controller } = setup();
    await next();
    const waiting = next();

    controller.abort();

    await expect(waiting).resolves.toBeUndefined();
  });

  it('rejects when the unblock fails', async () => {
    const { next, clientUnblock } = setup();
    clientUnblock.mockRejectedValueOnce(new Error('connection lost'));

    const act = next();

    await expect(act).rejects.toThrow('connection lost');
  });
});
