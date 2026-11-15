const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");

const templateSource = fs.readFileSync(
  path.join(__dirname, "templates", "miniapp", "index.html"),
  "utf8",
);

function functionSource(name, nextName) {
  const startMarker = `      function ${name}(`;
  const endMarker = `      function ${nextName}(`;
  const start = templateSource.indexOf(startMarker);
  const end = templateSource.indexOf(endMarker, start + startMarker.length);
  assert.notEqual(start, -1, `${name} must exist in the Mini App template`);
  assert.notEqual(end, -1, `${nextName} must follow ${name} in the Mini App template`);
  return templateSource.slice(start, end);
}

test("overview shows only active financial goals", () => {
  const accounts = [
    {
      id: "active",
      account_type: "savings",
      goal_amount: { value: "100" },
      balance: { value: "50" },
      goal_status: "active",
    },
    {
      id: "funded",
      account_type: "savings",
      goal_amount: { value: "100" },
      balance: { value: "112" },
      goal_status: "funded",
    },
    {
      id: "spent",
      account_type: "savings",
      goal_amount: { value: "100" },
      balance: { value: "0" },
      goal_status: "spent",
    },
  ];
  const overviewGoalsList = {
    children: [],
    hidden: true,
    replaceChildren() {
      this.children = [];
    },
    appendChild(child) {
      this.children.push(child);
    },
  };
  const overviewGoalsEmpty = { hidden: false };
  const context = vm.createContext({
    moneyState: { options: { accounts } },
    moneyElements: () => ({ overviewGoalsList, overviewGoalsEmpty }),
    goalAccountType: (account) => account.account_type,
    createGoalCard: (account) => ({ accountId: account.id }),
  });

  vm.runInContext(
    `${functionSource("goalMoneyValue", "goalScopeLabel")}\n${functionSource("renderOverviewGoals", "openGoalEditor")}\nthis.renderOverviewGoals = renderOverviewGoals;`,
    context,
  );
  context.renderOverviewGoals();

  assert.deepEqual(
    overviewGoalsList.children.map((child) => child.accountId),
    ["funded", "active"],
  );
  assert.equal(overviewGoalsList.hidden, false);
  assert.equal(overviewGoalsEmpty.hidden, true);
});

test("goal lifecycle keeps actual progress, including spent and overfunded balances", () => {
  const context = vm.createContext({});
  vm.runInContext(
    `${functionSource("goalMoneyValue", "goalScopeLabel")}\nthis.goalDisplayProgress = goalDisplayProgress; this.goalIsClosed = goalIsClosed; this.goalProgressWidth = goalProgressWidth;`,
    context,
  );

  for (const [status, balance, progress, closed] of [
    ["active", "625", 50, false],
    ["funded", "1400", 112, false],
    ["spent", "0", 0, true],
    ["completed", "250", 20, true],
  ]) {
    const account = { balance: { value: balance }, goal_amount: { value: "1250" }, goal_status: status };
    assert.equal(context.goalDisplayProgress(account), progress, status);
    assert.equal(context.goalIsClosed(account), closed, status);
    assert.equal(context.goalProgressWidth(progress), Math.min(100, progress), status);
  }
});
