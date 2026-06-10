import { spawn as nodeSpawn } from 'child_process';
import type { ChildProcess } from 'child_process';
import { EventEmitter } from 'events';
import readline from 'readline';
import { ALLOWED_TOOLS, SYSTEM_PROMPT, classifyClaudeLine, encodeUserTurn, ServerMessage } from './chat-protocol';

export type SpawnFn = typeof nodeSpawn;

export interface ClaudeProcessOptions {
  sessionId?: string;
  spawnFn?: SpawnFn;
  claudeBin?: string;
}

export class ClaudeProcess extends EventEmitter {
  private child: ChildProcess;
  private rl: readline.Interface | null = null;
  private stderr = '';

  constructor(opts: ClaudeProcessOptions = {}) {
    super();
    const spawnFn = opts.spawnFn ?? nodeSpawn;
    const bin = opts.claudeBin ?? 'claude';
    const args = [
      '-p',
      '--input-format', 'stream-json',
      '--output-format', 'stream-json',
      '--verbose',
      '--allowedTools', ALLOWED_TOOLS,
      '--append-system-prompt', SYSTEM_PROMPT,
    ];
    if (opts.sessionId) args.push('--resume', opts.sessionId);

    this.child = spawnFn(bin, args, { stdio: ['pipe', 'pipe', 'pipe'] });

    if (this.child.stdout) {
      this.rl = readline.createInterface({ input: this.child.stdout });
      this.rl.on('line', (line) => {
        for (const msg of classifyClaudeLine(line)) this.emit('message', msg);
      });
    }
    if (this.child.stderr) {
      this.child.stderr.on('data', (chunk) => {
        this.stderr += chunk.toString();
      });
    }
    this.child.on('error', (err: Error) => {
      this.emitMessage({ type: 'error', code: 'agent_unavailable', message: err.message });
    });
    this.child.on('exit', (code) => {
      if (code && code !== 0) {
        this.emitMessage({
          type: 'error',
          code: 'agent_failed',
          message: this.stderr.trim() || `claude exited with code ${code}`,
        });
      }
      this.emit('exit', code);
    });
  }

  private emitMessage(msg: ServerMessage): void {
    this.emit('message', msg);
  }

  send(text: string): void {
    this.child.stdin?.write(encodeUserTurn(text));
  }

  kill(): void {
    this.rl?.close();
    this.child.kill();
  }
}
