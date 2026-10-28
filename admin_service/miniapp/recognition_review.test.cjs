const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const {test} = require('node:test');
const html = fs.readFileSync(path.join(__dirname, 'templates/miniapp/index.html'), 'utf8');
test('field edits retain statement binding while invalidating the old draft', () => {
  const context = { batchId: 'batch-1', itemId: '1' };
  const state = { busy: false, draft: null, aiImageBatchContext: context, idempotencyKey: 'old' };
  const scope = { transactionState: state, transactionElements: () => ({}), renderAiImageBatch() {}, setTransactionConfirmationStatus() {} };
  vm.createContext(scope);
  vm.runInContext(source('resetTransactionDraft') + '\n' + source('invalidateTransactionDraft'), scope);
  scope.invalidateTransactionDraft();
  assert.equal(state.aiImageBatchContext, context);
  assert.equal(state.idempotencyKey, null);
  scope.resetTransactionDraft();
  assert.equal(state.aiImageBatchContext, null);
});
function source(name) {
  const start = html.indexOf(`      function ${name}(`);
  assert.notEqual(start, -1, `${name} exists`);
  return html.slice(start, html.indexOf('\n      function ', start + 1));
}
function sandbox(names, overrides = {}) {
  const state = {kind: 'expense', receiptItems: [], receiptMetadata: {source: 'receipt'}, options: {accounts: [{id: 7, currency: 'UAH'}]}};
  const c = vm.createContext(Object.assign({
    transactionState: state,
    recognitionReviewPayload: () => undefined,
    transactionElements: () => ({amount: {value: '100'}, account: {value: '7'}, category: {value: '8'}, date: {value: '2026-10-01'}, comment: {value: ''}}),
  }, overrides));
  vm.runInContext(names.map(source).join('\n'), c);
  return c;
}
test('receipt payload explicitly carries the selected account currency', () => {
  const c = sandbox(['transactionPayload']);
  assert.equal(c.transactionPayload().currency, 'UAH');
});

test('optional receipt lines require an explicit total-only choice, bound to review', () => {
  const c = sandbox(['transactionPayload', 'recognitionReviewValues']);
  assert.equal(c.transactionPayload().receipt_resolution, 'items');
  c.transactionState.receiptResolution = 'l1_only';
  const payload = c.transactionPayload();
  assert.equal(payload.receipt_resolution, 'l1_only');
  assert.equal(c.recognitionReviewValues(payload).receipt_resolution, 'l1_only');
});

test('acknowledgements bind to the exact edited values and disappear after an edit', () => {
  const c = sandbox(['recognitionReviewValues', 'recognitionReviewPayload']);
  c.transactionState.recognitionReview = {token: 'synthetic', issues: [{id: 'amount'}]};
  const payload = {kind: 'expense', amount: '100', currency: 'UAH', account_id: '7', category_id: '8', transaction_date: '2026-10-01', receipt_items: []};
  c.transactionState.recognitionAcknowledgements = {amount: JSON.stringify(c.recognitionReviewValues(payload))};
  assert.equal(c.recognitionReviewPayload(payload).acknowledgements.length, 1);
  assert.equal(c.recognitionReviewPayload({...payload, amount: '200'}).acknowledgements.length, 0);
});

test('batch acknowledgement preserves unsaved edits and trusts server acknowledgement', async () => {
  const batch = {batch_id: 'one', items: [{id: 'row', status: 'pending', suggestion: {amount: '130.00', comment: 'Edited'}}]};
  const requests = [];
  const c = sandbox(['acknowledgeAiImageBatchReview'], {
    aiImageBatchState: {batch, busy: false}, aiImageBatchItemUrl: '/batch',
    setAiImageBatchBusy: () => {}, setAiImageBatchStatus: () => {},
    ensureSession: () => Promise.resolve(), renderAiImageBatch: () => {},
    postJson: (url, payload) => {
      requests.push(payload);
      return Promise.resolve({batch: {batch_id: 'one', review_acknowledged: true, items: [{id: 'row', status: 'pending', suggestion: {amount: '100.00', comment: 'Original'}}]}});
    },
  });
  c.acknowledgeAiImageBatchReview();
  await new Promise(setImmediate);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].action, 'acknowledge_review');
  assert.equal(c.aiImageBatchState.batch.review_acknowledged, true);
  assert.equal(c.aiImageBatchState.batch.items[0].suggestion.amount, '130.00');
  assert.equal(c.aiImageBatchState.batch.items[0].suggestion.comment, 'Edited');
});

test('batch acknowledgement is visible for review rows and parser skipped rows', () => {
  assert.match(html, /Boolean\(batch\.review_rows && batch\.review_rows\.length\) \|\| Number\(batch\.skipped_items_count \|\| 0\) > 0/);
});
