import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // `.claude/` can hold worktree checkouts of this repo. They are gitignored,
    // but vitest's default glob would still collect their test files and report
    // a suite that is mostly other copies of itself.
    exclude: ["**/node_modules/**", "**/dist/**", "**/.claude/**"],
  },
});
