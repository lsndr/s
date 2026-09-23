import { DomainError } from './domain-error.js';

export abstract class DomainObject<S> {
  protected state: S;

  /**
   * @stateRecovery use only to recreate an instance from persisted state
   */
  public constructor(state: S) {
    this.state = state;
  }

  protected static assert(expression: boolean, key: string): void {
    if (!expression) {
      throw new DomainError(this.name, key);
    }
  }

  protected assert(expression: boolean, key: string): void {
    if (!expression) {
      throw new DomainError(this.constructor.name, key);
    }
  }
}
