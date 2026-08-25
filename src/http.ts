/**
 * The HTTP plumbing the Durable Streams adapters share. `fork.ts` and
 * `subscription.ts` both speak the protocol directly over `fetch` (the client
 * package covers neither surface), so the request shape, the status codes they
 * branch on, and the way they lift a server's explanation out of a failed
 * response live here rather than being spelled twice.
 */

/** Request options every direct-`fetch` surface in this package accepts. */
export type HttpEndpoint = Readonly<{
  /** HTTP headers for every request — typically `Authorization`. */
  headers?: Readonly<Record<string, string>>;
  /** Defaults to the global `fetch`; inject to add retries or instrumentation. */
  fetchClient?: typeof fetch;
}>;

export const STATUS_OK = 200;
export const STATUS_CREATED = 201;
export const STATUS_NO_CONTENT = 204;
export const STATUS_CONFLICT = 409;
export const STATUS_GONE = 410;

export const HEADER_CONTENT_TYPE = "Content-Type";
export const JSON_CONTENT_TYPE = "application/json";

/**
 * The server's explanation for a failed response, trimmed and safe to embed in
 * an error message. Empty when the response carried no body.
 */
export async function bodyText(response: Response): Promise<string> {
  return (await response.text()).trim();
}
