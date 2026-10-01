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
    goalMoneyValue: (payload) => Number(payload && payload.value || 0),
    goalProgress: (account) => Math.round((Number(account.balance.value) / Number(account.goal_amount.value)) * 100),
    goalIsClosed: (account) => account.goal_status === "funded" || account.goal_status === "spent",
    createGoalCard: (account) => ({ accountId: account.id }),
  });

  vm.runInContext(
    `${functionSource("renderOverviewGoals", "openGoalEditor")}\nthis.renderOverviewGoals = renderOverviewGoals;`,
    context,
  );
  context.renderOverviewGoals();

  assert.deepEqual(
    overviewGoalsList.children.map((child) => child.accountId),
    ["active"],
  );
  assert.equal(overviewGoalsList.hidden, false);
  assert.equal(overviewGoalsEmpty.hidden, true);
});
