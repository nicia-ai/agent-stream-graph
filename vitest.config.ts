import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    /**
     * The root suite is THIS package's tests, and only those.
     *
     * Without an explicit `include`, vitest's default glob also collects the
     * suites under `integrations/*` and `demos/*`. That looks like free
     * coverage and is not: those packages import the built entrypoint
     * (`@nicia-ai/agent-stream-graph` -> `dist/`), so folding them in here
     * makes `pnpm test` depend on `pnpm build` having run first. On a clean
     * checkout that fails outright, and — worse — on a dirty one it silently
     * tests whatever `dist/` happens to be lying around, so a change to
     * `src/` appears covered when the assertions never saw it.
     *
     * Each workspace package owns its own tests and runs them through its
     * `verify` script, in its own CI job, after the library is built. Use
     * `pnpm test:all` to build and then run every suite in one go.
     */
    include: ["test/**/*.test.ts"],
    // `.claude/` can hold worktree checkouts of this repo. They are gitignored,
    // but vitest's default glob would still collect their test files and report
    // a suite that is mostly other copies of itself.
    exclude: ["**/node_modules/**", "**/dist/**", "**/.claude/**"],
  },
});
