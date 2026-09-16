import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const dashboardBaseURL = process.argv[2]?.replace(/\/$/, "");
if (!dashboardBaseURL) {
  throw new Error(
    "usage: node scripts/verify-dashboard-esm.mjs <dashboard-base-url>",
  );
}

const moduleNames = [
  "index.js",
  "api.js",
  "inspector.js",
  "stream.js",
  "types.js",
];
const artifactDir = await mkdtemp(join(tmpdir(), "deadbolt-dashboard-esm-"));

try {
  // Node uses the nearest package.json to determine whether .js is ESM. This
  // mirrors the browser module graph while keeping DOM bootstrap disabled.
  await writeFile(join(artifactDir, "package.json"), '{"type":"module"}\n');

  for (const moduleName of moduleNames) {
    const response = await fetch(`${dashboardBaseURL}/${moduleName}`);
    if (!response.ok) {
      throw new Error(`GET ${moduleName} returned HTTP ${response.status}`);
    }
    await writeFile(join(artifactDir, moduleName), await response.text());
  }

  globalThis.document = undefined;
  await import(pathToFileURL(join(artifactDir, "index.js")).href);
  process.stdout.write(
    "Dashboard final-image ESM module graph imported successfully\n",
  );
} finally {
  await rm(artifactDir, { recursive: true, force: true });
}
