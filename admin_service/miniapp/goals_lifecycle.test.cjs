const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const file = path.join(__dirname, 'templates', 'miniapp', 'index.html');
const html = fs.readFileSync(file, 'utf8');
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
assert.ok(scripts.length, 'inline frontend script exists');
for (const script of scripts) new vm.Script(script, { filename: file });

assert.match(html, /id="goalDetailsCard"/);
assert.match(html, /id="goalDetailsName"/);
assert.match(html, /id="goalDetailsHistory"/);
for (const action of ['topup', 'edit', 'spend', 'complete']) assert.match(html, new RegExp(`data-goal-action="${action}"`));
assert.match(html, /action === "goal_create"/);
assert.match(html, /el\.accountCurrency\) el\.accountCurrency\.value = "UAH"/);
assert.match(html, /historyUrl \+ "\?account_id="/);
assert.match(html, /account\.goal_status !== "funded"/);
assert.match(html, /account\.goal_status === "spent"/);
assert.match(html, /goal_status === "spent"/);
assert.doesNotMatch(html, /goalAccounts\[0\]/, 'global edit does not silently pick first goal');
assert.doesNotMatch(html, /goalAccounts\.find\([\s\S]*?\) \|\| goalAccounts\[0\]/);
console.log('goals lifecycle frontend behavioral checks: PASS');

function source(name) {
  const start = html.indexOf(`      function ${name}(`);
  assert.notEqual(start, -1, `real function ${name} exists`);
  const end = html.indexOf('\n      function ', start + 1);
  return html.slice(start, end);
}
function context(names, extras = {}) {
  const sandbox = vm.createContext({...extras});
  vm.runInContext(names.map(source).join('\n'), sandbox);
  return sandbox;
}
const {test} = require('node:test');
const goal = {id: 41, label: 'Dedicated goal account', goal_name: 'Holiday', goal_status: 'funded',
  goal_amount: {value: '200.00', display: '200 UAH'}, balance: {value: '275.00', display: '275 UAH'}};
function cards() {
  return context(['goalMoneyValue', 'goalProgress', 'goalProgressWidth', 'goalIsClosed', 'goalIsSpent', 'goalDisplayProgress', 'createGoalCard'], {
    document: {createElement: () => ({dataset: {}, listeners: {}, addEventListener(name, fn) {this.listeners[name] = fn;}})},
    goalEscapeHtml: (value) => value, goalScopeLabel: () => 'Personal', tr: (key) => key,
    shortDate: (value) => value, openGoalDetails: () => {},
  });
}
test('funded over-target goal stays open and shows actual balance and progress', () => {
  const c = cards();
  assert.equal(c.goalIsClosed(goal), false);
  const card = c.createGoalCard(goal, false);
  assert.match(card.innerHTML, /275 UAH/);
  assert.match(card.innerHTML, /138%/);
  assert.match(card.innerHTML, /width:100%/);
});
test('completed goal is closed without replacing its actual balance with target', () => {
  const c = cards();
  assert.equal(c.goalIsClosed({...goal, goal_status: 'completed'}), true);
  assert.match(c.createGoalCard({...goal, goal_status: 'completed'}, false).innerHTML, /275 UAH/);
});
test('click opens details for the exact goal, not an implicit top-up', () => {
  let selected;
  const c = cards();
  c.openGoalDetails = (account) => {selected = account.id;};
  c.createGoalCard(goal, false).listeners.click();
  assert.equal(selected, 41);
});
test('completion opens a visible confirmation editor for the selected goal', async () => {
  const el = {accountAction: {value: ''}, accountTarget: {value: ''}};
  const calls = [];
  const c = context(['completeGoal'], {moneyElements: () => el, goalIsSpent: () => false, goalIsClosed: () => false,
    showScreen: async (screen) => calls.push(screen), setMoneyMode: (mode) => calls.push(mode),
    updateAccountActionFields: () => {}, resetAccountDraft: () => {}, submitAccountDraft: async () => calls.push('draft')});
  await c.completeGoal(goal);
  assert.equal(el.accountAction.value, 'goal_complete');
  assert.equal(el.accountTarget.value, '41');
  assert.ok(calls.includes('accounts'), 'confirmation must be visible');
  assert.ok(calls.includes('draft'));
});
test('submission preserves edits across options hydration', async () => {
  let editedName = 'edited';
  let posted;
  const state = context(['submitAccountDraft'], {
    moneyElements: () => ({accountForm: {}}),
    moneyState: {accountBusy: false}, resetAccountDraft: () => {},
    setMoneyBusy: () => {}, setMoneyStatus: () => {},
    accountDraftUrl: '/draft', makeIdempotencyKey: () => 'key',
    accountPayload: () => ({goal_name: editedName}),
    loadMoneyOptions: () => { editedName = 'original'; return Promise.resolve(); },
    postJson: (url, body) => { posted = body; return Promise.resolve({draft: {}}); },
    renderAccountDraft: () => {}, transactionErrorMessage: String
  });
  state.submitAccountDraft();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(posted.goal_name, 'edited');
});

test('global edit requires selecting a goal instead of editing prior top-up', async () => {
  let opened;
  const c = context(['openGoalAdjust'], {moneyState: {goalTopUpAccountId: '41', options: {accounts: [goal]}},
    goalAccountType: () => 'savings', goalMoneyValue: () => 200, openGoalEditor: () => {opened = true;},
    openGoalCreate: () => {opened = true;}, showScreen: async () => {}, setMoneyMode: () => {},
    showStatus: () => {}, tr: (key) => key});
  await c.openGoalAdjust();
  assert.equal(opened, undefined);
});
