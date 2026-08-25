/** The kind of change a shape row underwent. Electric change messages use
 * lowercase `operation` values; this is the subset this library projects. */
export type Operation = "insert" | "update" | "delete";

/**
 * One durable change from a shape log (Electric-style). `offset` is the
 * resumable position; Electric-style numeric tuple offsets such as
 * `"<lsn>_<op_position>"` are compared numerically, so zero padding is not
 * required. Offsets within a single stream must be consistently numeric-tuple
 * OR consistently non-numeric; {@link compareOffsets} throws on a mix.
 */
export type ShapeChange<Value = Record<string, unknown>> = Readonly<{
  offset: string;
  shape: string;
  key: string;
  operation: Operation;
  value: Value;
}>;

/**
 * What a source's change mapper returns: a {@link ShapeChange} minus `offset`.
 *
 * The offset is the adapter's to assign, never the mapper's — only the adapter
 * knows what position in its transport a message occupies, and a mapper that
 * could set it could checkpoint a position the stream cannot be resumed from.
 */
export type ShapeChangeInput<Value = Record<string, unknown>> = Omit<ShapeChange<Value>, "offset">;
