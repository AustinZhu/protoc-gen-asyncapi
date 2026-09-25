// Validates AsyncAPI documents with the official parser (spec schema plus the
// parser's built-in Spectral ruleset). Usage: node validate.mjs <file>...
// With no arguments, validates every golden document under
// ../../internal/generator/testdata/golden and the example documents.
// Exits non-zero if any document has an error-severity diagnostic.
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Parser, DiagnosticSeverity } from '@asyncapi/parser';

const here = dirname(fileURLToPath(import.meta.url));

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir).sort()) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) out.push(...walk(p));
    else if (/\.(ya?ml|json)$/.test(name)) out.push(p);
  }
  return out;
}

function defaultFiles() {
  return [
    ...walk(resolve(here, '../../internal/generator/testdata/golden')),
    ...walk(resolve(here, '../../examples/asyncapi')),
  ];
}

const files = process.argv.length > 2 ? process.argv.slice(2) : defaultFiles();
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
