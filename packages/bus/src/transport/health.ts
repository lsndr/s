export interface Health {
  assertHealthy(): Promise<void>;
}

export const Health = Symbol('Health');
