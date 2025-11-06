const { Command } = require('commander');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const cli = new Command();

const DATA_FILE = process.env.TASKFLOW_DB || path.join(process.cwd(), 'taskdata.json');
const WORKERS_FILE = path.join(process.cwd(), 'workers.json');
const DEFAULT_CONF = { max_retries: 3, backoff_base: 2 };

const timestamp = () => new Date().toISOString();

function isObject(o) { return o && typeof o === 'object' && !Array.isArray(o); }

function readTextFileStripBOM(p) {
  let txt = fs.readFileSync(p, 'utf8');
  if (txt.charCodeAt(0) === 0xFEFF) txt = txt.slice(1);
  return txt;
}

function loadData() {
  try {
    if (!fs.existsSync(DATA_FILE)) return { tasks: [], config: DEFAULT_CONF };
    const text = readTextFileStripBOM(DATA_FILE);
    const raw = JSON.parse(text);
    if (isObject(raw) && Array.isArray(raw.jobs)) {
      const tasks = raw.jobs.map(j => ({
        id: j.id,
        command: j.command,
        state: (j.state === 'pending' ? 'waiting' :
               j.state === 'processing' ? 'running' :
               j.state === 'completed' ? 'done' :
               j.state === 'dead' ? 'failed' : j.state || 'waiting'),
        attempts: j.attempts || 0,
        max_retries: j.max_retries != null ? j.max_retries : DEFAULT_CONF.max_retries,
        created_at: j.created_at,
        updated_at: j.updated_at,
        next_run_at: j.next_attempt_at || null,
        last_error: j.last_error || null,
        exit_code: j.exit_code || null
      }));
      const config = isObject(raw.config) ? raw.config : DEFAULT_CONF;
      const migrated = { tasks, config };
      saveData(migrated);
      return migrated;
    }

    if (isObject(raw) && Array.isArray(raw.tasks)) {
      raw.config = raw.config && isObject(raw.config) ? raw.config : DEFAULT_CONF;
      return raw;
    }
    console.warn('Warning: data file has unexpected shape, using empty dataset (file preserved).');
    return { tasks: [], config: DEFAULT_CONF };
  } catch (e) {
    console.error('Error loading data file (using empty dataset):', e.message);
    return { tasks: [], config: DEFAULT_CONF };
  }
}

function saveData(data) {
  const tmp = DATA_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
  fs.renameSync(tmp, DATA_FILE);
}

function saveWorkersMeta(meta) {
  const tmp = WORKERS_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(meta, null, 2), 'utf8');
  fs.renameSync(tmp, WORKERS_FILE);
}
function loadWorkersMeta() {
  try {
    if (!fs.existsSync(WORKERS_FILE)) return { workers: [] };
    return JSON.parse(readTextFileStripBOM(WORKERS_FILE));
  } catch (e) {
    return { workers: [] };
  }
}

function initData() {
  if (!fs.existsSync(DATA_FILE)) {
    const db = { tasks: [], config: DEFAULT_CONF };
    saveData(db);
    console.log('Database created at', DATA_FILE);
  } else {
    console.log('Database already exists at', DATA_FILE);
  }
}

function addTaskObject(task) {
  if (!task || !task.command) throw new Error('Task must include a "command" field');
  const data = loadData();
  const id = task.id || uuidv4();
  const now = timestamp();
  const entry = {
    id,
    command: task.command,
    state: task.state || 'waiting',
    attempts: Number(task.attempts || 0),
    max_retries: Number(task.max_retries ?? data.config.max_retries ?? DEFAULT_CONF.max_retries),
    created_at: task.created_at || now,
    updated_at: task.updated_at || now,
    next_run_at: task.next_run_at || null,
    last_error: task.last_error || null,
    exit_code: task.exit_code || null
  };
  const idx = data.tasks.findIndex(t => t.id === id);
  if (idx >= 0) data.tasks[idx] = entry;
  else data.tasks.push(entry);
  saveData(data);
  return id;
}

function addTask(jsonString) {
  let task;
  try { task = JSON.parse(jsonString); } catch (e) { throw new Error('Invalid JSON: ' + e.message); }
  return addTaskObject(task);
}

function claimTask() {
  const data = loadData();
  if (!data || !Array.isArray(data.tasks)) return null;
  const now = timestamp();
  const available = data.tasks
    .filter(t => t.state === 'waiting' && (!t.next_run_at || t.next_run_at <= now))
    .sort((a, b) => (a.created_at || '').localeCompare(b.created_at || ''));
  if (available.length === 0) return null;
  const task = available[0];
  task.state = 'running';
  task.updated_at = timestamp();
  saveData(data);
  return task;
}

function markDone(id) {
  const data = loadData();
  const t = data.tasks.find(t => t.id === id);
  if (!t) return;
  t.state = 'done';
  t.exit_code = 0;
  t.updated_at = timestamp();
  saveData(data);
}

function markFailed(id, attempts, maxRetries, code, err, base) {
  const data = loadData();
  const t = data.tasks.find(t => t.id === id);
  if (!t) return;
  t.attempts = Number(attempts) + 1;
  t.last_error = String(err);
  t.exit_code = code;
  if (t.attempts >= t.max_retries) {
    t.state = 'failed';
    t.updated_at = timestamp();
  } else {
    const delay = Math.pow(Number(base || data.config.backoff_base || DEFAULT_CONF.backoff_base), t.attempts);
    t.next_run_at = new Date(Date.now() + delay * 1000).toISOString();
    t.state = 'waiting';
    t.updated_at = timestamp();
  }
  saveData(data);
}

function execCommand(cmd) {
  return new Promise((resolve, reject) => {
    const proc = spawn(cmd, { shell: true, stdio: 'inherit' });
    proc.on('error', reject);
    proc.on('exit', code => resolve(code === null ? -1 : code));
  });
}

async function workerLoopOnce(shutdownSignal) {
  while (!shutdownSignal()) {
    const task = claimTask();
    if (!task) {
      await delay(1000);
      continue;
    }
    console.log(`Picked task ${task.id}: ${task.command}`);
    const db = loadData();
    const base = db.config.backoff_base || DEFAULT_CONF.backoff_base;
    try {
      const code = await execCommand(task.command);
      if (code === 0) {
        markDone(task.id);
        console.log(`Task ${task.id} completed`);
      } else {
        markFailed(task.id, task.attempts, task.max_retries, code, `Exit ${code}`, base);
        console.log(`Task ${task.id} failed (exit ${code}), attempts=${Number(task.attempts)+1}`);
      }
    } catch (err) {
      markFailed(task.id, task.attempts, task.max_retries, -1, String(err), base);
      console.log(`Task ${task.id} failed with exception:`, err);
    }
  }
}

function spawnWorkers(count) {
  const procs = [];
  for (let i = 0; i < count; i++) {
    const env = Object.assign({}, process.env, { TASKFLOW_CHILD: '1' });
    const childScript = path.join(__dirname, path.basename(__filename));
    const child = spawn(process.execPath, [childScript, '--child'], {
      env,
      stdio: ['ignore', 'inherit', 'inherit']
    });
    console.log(`Started worker pid=${child.pid}`);
    procs.push(child);
  }
  const meta = { workers: procs.map(p => ({ pid: p.pid, started_at: timestamp() })) };
  saveWorkersMeta(meta);
  return procs;
}

function stopWorkersFromMeta() {
  const meta = loadWorkersMeta();
  if (!meta || !Array.isArray(meta.workers) || meta.workers.length === 0) {
    console.log('No worker metadata found');
    return;
  }
  meta.workers.forEach(w => {
    try {
      process.kill(w.pid, 'SIGINT');
      console.log(`Sent SIGINT to pid ${w.pid}`);
    } catch (e) {
      console.error(`Could not signal pid ${w.pid}:`, e.message);
    }
  });
  try { fs.unlinkSync(WORKERS_FILE); } catch (e) {  }
}

async function runAsChild() {
  console.log(`TaskFlow child worker started (pid=${process.pid})`);
  let shuttingDown = false;
  process.on('SIGINT', () => { shuttingDown = true; });
  await workerLoopOnce(() => shuttingDown);
  console.log(`TaskFlow child worker exiting (pid=${process.pid})`);
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

function readJsonFromFileSync(filePath) {
  let txt = fs.readFileSync(filePath, 'utf8');
  if (txt.charCodeAt(0) === 0xFEFF) txt = txt.slice(1);
  return txt;
}
function readJsonFromStdin() {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', chunk => data += chunk);
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', err => reject(err));
  });
}

cli.name('taskflow').description('TaskFlow CLI - local task queue manager');

cli.command('init').description('Initialize task storage').action(() => initData());

cli
  .command('add [json]')
  .description('Add new task JSON. Use --file <path> or - for stdin input')
  .option('--file <path>', 'Read task JSON from file')
  .action(async (jsonArg, opts) => {
    try {
      let payload = null;
      if (opts.file) {
        if (!fs.existsSync(opts.file)) {
          console.error('File not found:', opts.file);
          process.exit(1);
        }
        payload = readJsonFromFileSync(opts.file);
      } else if (jsonArg === '-' || jsonArg === undefined) {
        if (jsonArg === '-') payload = await readJsonFromStdin();
        else {
          console.error('No JSON provided. Use --file <path> or pass JSON as argument or "-" to read from stdin.');
          process.exit(1);
        }
      } else {
        payload = jsonArg;
      }
      if (!payload || payload.trim() === '') {
        throw new Error('Empty input received');
      }
      const id = addTask(payload);
      console.log('Task added with id', id);
    } catch (e) {
      console.error('Add failed:', e.message);
      process.exit(1);
    }
  });

const workerCmd = cli.command('worker').description('Worker management');

workerCmd
  .command('start')
  .description('Start workers in foreground (parent manages them)')
  .option('--count <n>', 'Number of worker processes', '1')
  .action((opts) => {
    initData();
    const count = Number(opts.count || 1);
    if (count <= 0) {
      console.error('Invalid count'); process.exit(1);
    }
    const procs = spawnWorkers(count);
    let shutting = false;
    process.on('SIGINT', () => {
      if (shutting) return;
      shutting = true;
      console.log('Parent: stopping workers...');
      procs.forEach(p => {
        try { process.kill(p.pid, 'SIGINT'); } catch (e) {}
      });
      setTimeout(() => process.exit(0), 5000);
    });
    procs.forEach(p => {
      p.on('exit', (code, sig) => {
        console.log(`Worker pid=${p.pid} exited code=${code} sig=${sig}`);
      });
    });
  });

workerCmd
  .command('stop')
  .description('Stop running workers (using recorded PIDs)')
  .action(() => {
    stopWorkersFromMeta();
  });

cli
  .command('worker:foreground')
  .description('Run a single worker in foreground (legacy alias)')
  .action(() => {
    initData();
    let shutting = false;
    process.on('SIGINT', () => { console.log('Stopping worker...'); shutting = true; });
    workerLoopOnce(() => shutting).then(() => console.log('Worker stopped.'));
  });

cli
  .option('--child', 'Run single worker loop as child process');

const configCmd = cli.command('config').description('Configuration');

configCmd.command('set <key> <value>').action((key, value) => {
  const data = loadData();
  data.config = data.config && isObject(data.config) ? data.config : {};
  const asNumber = /^\d+$/.test(value) ? Number(value) : value;
  data.config[key] = asNumber;
  saveData(data);
  console.log(`Set ${key}=${asNumber}`);
});

configCmd.command('get <key>').action((key) => {
  const data = loadData();
  const v = data.config && data.config[key];
  if (v === undefined) console.log('Not set');
  else console.log(String(v));
});

cli.command('status').description('Show task counts & workers').action(() => {
  const data = loadData();
  const counts = Array.isArray(data.tasks) ? data.tasks.reduce((a, t) => { a[t.state] = (a[t.state]||0)+1; return a; }, {}) : {};
  console.log('Tasks summary:', counts);
  console.log('Data file:', DATA_FILE);
  const w = loadWorkersMeta();
  console.log('Workers metadata:', w);
});

cli.command('list').option('--state <s>').description('List tasks optionally filtered').action((opts) => {
  const data = loadData();
  const rows = opts.state ? (data.tasks || []).filter(t => t.state === opts.state) : (data.tasks || []);
  rows.forEach(r => console.log(JSON.stringify(r)));
});

const dlq = cli.command('dead').description('Dead task management');
dlq.command('list').action(() => {
  const data = loadData();
  (data.tasks || []).filter(t => t.state === 'failed').forEach(r => console.log(JSON.stringify(r)));
});
dlq.command('retry <id>').action((id) => {
  const data = loadData();
  if (!Array.isArray(data.tasks)) { console.error('No tasks found'); process.exit(1); }
  const t = data.tasks.find(t => t.id === id && t.state === 'failed');
  if (!t) { console.error('No such task in failed list'); process.exit(1); }
  t.state = 'waiting'; t.attempts = 0; t.next_run_at = null; t.updated_at = timestamp();
  saveData(data);
  console.log('Moved task back to waiting:', id);
});

cli.command('selfcheck').description('Run lightweight selfcheck').action(() => {
  console.log('Running selfcheck...');
  const testFile = DATA_FILE + '.selftest.json';
  if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
  const orig = process.env.TASKFLOW_DB;
  try {
    process.env.TASKFLOW_DB = testFile;
    initData();
    const id = addTask(JSON.stringify({ command: 'echo test', max_retries: 1 }));
    console.log('Added test task', id);
    const db = loadData();
    const t = db.tasks[0];
    markFailed(t.id, t.attempts, t.max_retries, 1, 'simulate', 2);
    const s = loadData().tasks[0].state;
    console.log(s === 'failed' ? 'Selfcheck passed' : 'Selfcheck failed: ' + s);
  } finally {
    if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
    if (orig === undefined) delete process.env.TASKFLOW_DB;
    else process.env.TASKFLOW_DB = orig;
  }
});

(async () => {
  if (process.argv.includes('--child') || process.env.TASKFLOW_CHILD === '1') {
    try {
      await runAsChild();
      process.exit(0);
    } catch (err) {
      console.error('Child worker error:', err);
      process.exit(1);
    }
  }
  cli.parse(process.argv);
  const invoked = process.argv.slice(2);
  if (invoked.length === 1 && invoked[0] === 'worker') {
    initData();
    let shutting = false;
    process.on('SIGINT', () => { console.log('Stopping worker...'); shutting = true; });
    await workerLoopOnce(() => shutting);
    console.log('Worker stopped.');
  }
})();
