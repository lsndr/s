/** Internal counterpart of the bus `OnBusShutdown` for the transport parts. */
export interface OnTransportShutdown {
  onTransportShutdown(): Promise<void> | void;
}
