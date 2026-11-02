import { readFileSync } from 'fs';

// One credential, given as the environment variable <name> or as a file named
// by <name>_FILE. A variable that is unset or empty counts as absent. The file
// is read on every call, so a rotated credential is picked up by the next
// read; the file's content loses exactly one trailing newline ("\n" or "\r\n")
// and nothing else. Returns undefined when neither form is set.
export function readSecret(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const fileVar = `${name}_FILE`;
  const value = env[name] || undefined;
  const file = env[fileVar] || undefined;
  if (value !== undefined && file !== undefined) {
    throw new Error(`${name}: set ${name} or ${fileVar}, not both`);
  }
  if (file === undefined) return value;

  let content: string;
  try {
    content = readFileSync(file, 'utf8');
  } catch (err) {
    throw new Error(`${fileVar}: ${(err as Error).message}`);
  }
  if (content === '') throw new Error(`${fileVar}: ${file} is empty`);
  return content.replace(/\r?\n$/, '');
}
