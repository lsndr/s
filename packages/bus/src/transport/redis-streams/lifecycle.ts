/** Internal counterpart of the bus `OnBusShutdown` for the transport parts. */
export interface OnTransportShutdown {
  /** Called when the transport is shut down by the bus. */
  onTransportShutdown(): Promise<void> | void;
}
