const { Command } = require('commander');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const cli = new Command();

const DATA_FILE = process.env.TASKFLOW_DB || path.join(process.cwd(), 'taskdata.json');
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
      const tasks = raw.jobs.map(j => {
        return {
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
        };
      });
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
  if (t.attempts > t.max_retries) {
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

async function runWorker() {
  console.log('TaskFlow worker running Press Ctrl-C to stop.');
  let active = true;
  process.on('SIGINT', () => { console.log('Stopping worker'); active = false; });
  while (active) {
    const task = claimTask();
    if (!task) {
      await delay(1000);
      continue;
    }
    console.log(`Running task ${task.id}: ${task.command}`);
    const db = loadData();
    const base = db.config.backoff_base || DEFAULT_CONF.backoff_base;
    try {
      const code = await execCommand(task.command);
      if (code === 0) {
        markDone(task.id);
        console.log(`Task ${task.id} finished successfully`);
      } else {
        markFailed(task.id, task.attempts, task.max_retries, code, `Exit ${code}`, base);
        console.log(`Task ${task.id} failed (exit ${code}), retry ${task.attempts + 1}`);
      }
    } catch (err) {
      markFailed(task.id, task.attempts, task.max_retries, -1, String(err), base);
      console.log(`Task ${task.id} crashed:`, err);
    }
  }
  console.log('Worker stopped.');
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

cli.command('init').description('Initialize task storage').action(() => initData());

cli
  .command('add [json]')
  .description('Add new task JSON. Use --file <path> or - for stdin input')
  .option('-file <path>', 'Read task JSON from file')
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

cli.command('worker').description('Run foreground task worker').action(() => {
  initData();
  runWorker();
});

cli.command('status').description('Show task summary').action(() => {
  const data = loadData();
  const summary = Array.isArray(data.tasks) ? data.tasks.reduce((a, t) => { a[t.state] = (a[t.state] || 0) + 1; return a; }, {}) : {};
  console.log('Task states:', summary);
  console.log('Data file:', DATA_FILE);
});

cli.command('list').option('--state <s>').description('List stored tasks').action(opts => {
  const data = loadData();
  const tasks = opts.state ? (data.tasks || []).filter(t => t.state === opts.state) : (data.tasks || []);
  tasks.forEach(t => console.log(JSON.stringify(t)));
});

const dlq = cli.command('dead').description('Dead task management');
dlq.command('list').action(() => {
  const data = loadData();
  (data.tasks || []).filter(t => t.state === 'failed').forEach(t => console.log(JSON.stringify(t)));
});
dlq.command('retry <id>').action(id => {
  const data = loadData();
  if (!Array.isArray(data.tasks)) { console.error('No tasks found'); process.exit(1); }
  const t = data.tasks.find(t => t.id === id && t.state === 'failed');
  if (!t) { console.error('No such task in failed list'); process.exit(1); }
  t.state = 'waiting'; t.attempts = 0; t.next_run_at = null; t.updated_at = timestamp();
  saveData(data);
  console.log('Moved task back to waiting:', id);
});

cli.command('selfcheck').description('Run quick verification').action(() => {
  console.log('Running internal check...');
  const testFile = DATA_FILE + '.test.json';
  if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
  const orig = DATA_FILE;
  try {
    process.env.TASKFLOW_DB = testFile;
    initData();
    const id = addTask(JSON.stringify({ command: 'echo test', max_retries: 1 }));
    console.log('Added test task', id);
    const db = loadData();
    const t = db.tasks[0];
    markFailed(t.id, t.attempts, t.max_retries, 1, 'simulate', 2);
    const s = loadData().tasks[0].state;
    console.log(s === 'failed' ? 'Check passed' : 'Check failed: ' + s);
  } finally {
    if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
    process.env.TASKFLOW_DB = orig;
  }
});

cli.parse(process.argv);
