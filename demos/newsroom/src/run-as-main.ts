/** Run `main()` when this module is the process entry point; exit non-zero on error. */
import { pathToFileURL } from "node:url";

export function runAsMain(importMetaUrl: string, main: () => Promise<void>): void {
  if (importMetaUrl === pathToFileURL(process.argv[1] ?? "").href) {
    main().catch((error: unknown) => {
      console.error(error);
      process.exit(1);
    });
  }
}
