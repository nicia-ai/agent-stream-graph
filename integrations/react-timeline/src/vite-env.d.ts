interface ImportMetaEnv {
  /** "mock" (default) or "electric". See collections.ts. */
  readonly VITE_ASG_MODE?: string;
  /** Proxy URL for the belief-versions Electric shape. Required when VITE_ASG_MODE=electric. */
  readonly VITE_ASG_VERSIONS_URL?: string;
  /** Proxy URL for the recorded-anchors Electric shape. Required when VITE_ASG_MODE=electric. */
  readonly VITE_ASG_ANCHORS_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
