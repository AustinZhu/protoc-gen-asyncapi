// Validates AsyncAPI documents with the official parser (spec schema plus the
// parser's built-in Spectral ruleset). Usage: node validate.mjs <file>...
// With no arguments, validates every golden document under ../../testdata.
// Exits non-zero if any document has an error-severity diagnostic.
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Parser, DiagnosticSeverity } from '@asyncapi/parser';

const here = dirname(fileURLToPath(import.meta.url));

function goldenFiles() {
  const root = resolve(here, '../../testdata');
  const out = [];
  for (const dir of readdirSync(root)) {
    for (const name of ['expected.asyncapi.yaml', 'expected.asyncapi.json']) {
      const p = join(root, dir, name);
      if (existsSync(p)) out.push(p);
    }
  }
  return out;
}

const files = process.argv.length > 2 ? process.argv.slice(2) : goldenFiles();
const parser = new Parser();
let failed = false;
for (const file of files) {
  const { document, diagnostics } = await parser.parse(readFileSync(file, 'utf8'), { source: file });
  const errors = diagnostics.filter((d) => d.severity === DiagnosticSeverity.Error);
  const warnings = diagnostics.filter((d) => d.severity === DiagnosticSeverity.Warning);
  for (const d of [...errors, ...warnings]) {
    const level = d.severity === DiagnosticSeverity.Error ? 'error' : 'warning';
    console.log(`  ${level} ${d.code} at ${d.path.join('.')}: ${d.message}`);
  }
  if (!document || errors.length > 0) {
    failed = true;
    console.log(`FAIL ${file}`);
  } else {
    console.log(`ok   ${file} (${document.operations().length} operations, ${warnings.length} warnings)`);
  }
}
process.exit(failed ? 1 : 0);
