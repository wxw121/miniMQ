const fields = {
  mode: document.querySelector('#mode'),
  bootstrapServers: document.querySelector('#bootstrapServers'),
  topic: document.querySelector('#topic'),
  message: document.querySelector('#message'),
  key: document.querySelector('#key'),
  groupId: document.querySelector('#groupId'),
  partitions: document.querySelector('#partitions'),
  button: document.querySelector('#runFlow'),
  status: document.querySelector('#status'),
  modeResult: document.querySelector('#modeResult'),
  offset: document.querySelector('#offset'),
  log: document.querySelector('#flowRawJson'),
  error: document.querySelector('#flowError')
};

const operatorFields = {
  broker: document.querySelector('#operatorBroker'),
  limit: document.querySelector('#limit'),
  partitionLimit: document.querySelector('#partitionLimit'),
  replicationFactor: document.querySelector('#replicationFactor'),
  targetPartitionCount: document.querySelector('#targetPartitionCount'),
  includeDeleted: document.querySelector('#includeDeleted'),
  retryTopic: document.querySelector('#retryTopic'),
  deadLetterTopic: document.querySelector('#deadLetterTopic'),
  clearRetry: document.querySelector('#clearRetry'),
  clearDeadLetter: document.querySelector('#clearDeadLetter'),
  brokerId: document.querySelector('#brokerId'),
  deliverAtEpochMs: document.querySelector('#deliverAtEpochMs'),
  status: document.querySelector('#operatorStatus'),
  endpoint: document.querySelector('#operatorEndpoint'),
  log: document.querySelector('#rawJson'),
  structured: document.querySelector('#structuredResult'),
  error: document.querySelector('#requestError')
};

const operatorButtons = [...document.querySelectorAll('[data-endpoint]')];
const viewButtons = [...document.querySelectorAll('[data-view-target]')];
const views = [...document.querySelectorAll('[data-view]')];
const connectionSummary = document.querySelector('#connectionSummary');
const connectionState = document.querySelector('#connectionState');
const overviewConfiguration = document.querySelector('#overviewConfiguration');
const overviewBroker = document.querySelector('#overviewBroker');
const overviewLastRequest = document.querySelector('#overviewLastRequest');
const viewResultState = new Map();
let activeViewName = 'overview';
let hasSuccessfulFlowResult = false;

function activateView(name) {
  const target = views.find((view) => view.dataset.view === name);
  if (!target) {
    return;
  }
  views.forEach((view) => {
    view.hidden = view !== target;
  });
  viewButtons.forEach((button) => {
    if (button.dataset.viewTarget === name) {
      button.setAttribute('aria-current', 'page');
    } else {
      button.removeAttribute('aria-current');
    }
  });
  const label = viewButtons.find((button) => button.dataset.viewTarget === name)?.textContent.trim();
  document.title = `${label || 'Operator'} · MiniMQ Operator Console`;
  activeViewName = name;
  restoreOperatorResult(name);
}

function displayName(value) {
  return String(value)
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/^./, (character) => character.toUpperCase());
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function scalarText(value) {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

function renderObjectTable(rows) {
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const wrapper = document.createElement('div');
  wrapper.className = 'result-table-wrap';
  const table = document.createElement('table');
  table.className = 'result-table';
  const thead = document.createElement('thead');
  const headingRow = document.createElement('tr');
  columns.forEach((column) => {
    const heading = document.createElement('th');
    heading.scope = 'col';
    heading.textContent = displayName(column);
    headingRow.append(heading);
  });
  thead.append(headingRow);
  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tableRow = document.createElement('tr');
    columns.forEach((column) => {
      const cell = document.createElement('td');
      cell.textContent = scalarText(row[column]);
      tableRow.append(cell);
    });
    tbody.append(tableRow);
  });
  table.append(thead, tbody);
  wrapper.append(table);
  return wrapper;
}

function renderObject(value) {
  const entries = Object.entries(value);
  if (entries.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'empty-state';
    empty.textContent = 'No data returned.';
    return empty;
  }

  const container = document.createElement('div');
  const scalarEntries = entries.filter(([, entry]) => !entry || typeof entry !== 'object');
  const nestedEntries = entries.filter(([, entry]) => entry && typeof entry === 'object');
  if (scalarEntries.length > 0) {
    const list = document.createElement('dl');
    list.className = 'result-grid';
    scalarEntries.forEach(([key, entry]) => {
      const field = document.createElement('div');
      field.className = 'result-field';
      const term = document.createElement('dt');
      term.textContent = displayName(key);
      const description = document.createElement('dd');
      description.textContent = scalarText(entry);
      field.append(term, description);
      list.append(field);
    });
    container.append(list);
  }
  nestedEntries.forEach(([key, entry]) => {
    const group = document.createElement('section');
    group.className = 'result-group';
    const heading = document.createElement('h3');
    heading.textContent = displayName(key);
    group.append(heading, renderValue(entry));
    container.append(group);
  });
  return container;
}

function renderValue(value) {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      const empty = document.createElement('p');
      empty.className = 'empty-state';
      empty.textContent = 'No data returned.';
      return empty;
    }
    if (value.every(isPlainObject)) {
      return renderObjectTable(value);
    }
    const list = document.createElement('ul');
    list.className = 'primitive-list';
    value.forEach((entry) => {
      const item = document.createElement('li');
      item.textContent = scalarText(entry);
      list.append(item);
    });
    return list;
  }
  if (isPlainObject(value)) {
    return renderObject(value);
  }
  const text = document.createElement('p');
  text.textContent = scalarText(value);
  return text;
}

function renderStructuredResult(result) {
  operatorFields.structured.classList.remove('empty-state');
  operatorFields.structured.replaceChildren(renderValue(result));
  operatorFields.log.textContent = JSON.stringify(result, null, 2);
  operatorFields.error.hidden = true;
}

function restoreOperatorResult(viewName) {
  const state = viewResultState.get(viewName);
  operatorFields.error.hidden = true;
  if (!state) {
    operatorFields.status.textContent = 'IDLE';
    operatorFields.status.dataset.state = 'idle';
    operatorFields.endpoint.textContent = 'No request';
    operatorFields.structured.classList.add('empty-state');
    operatorFields.structured.textContent = 'Run an operator action to see its result.';
    operatorFields.log.textContent = 'No operator result.';
    return;
  }
  operatorFields.status.textContent = 'PASS';
  operatorFields.status.dataset.state = 'pass';
  operatorFields.endpoint.textContent = `POST ${state.endpoint}`;
  renderStructuredResult(state.result);
}

function requiredValue(input, label) {
  const value = input.value.trim();
  if (!value) {
    throw new Error(`${label} is required.`);
  }
  return value;
}

function numericValue(input, label) {
  const rawValue = input.value.trim();
  const value = Number(rawValue);
  const minimum = input.min === '' ? Number.MIN_SAFE_INTEGER : Number(input.min);
  const maximum = input.max === '' ? Number.MAX_SAFE_INTEGER : Number(input.max);
  if (!rawValue || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${label} is invalid.`);
  }
  return value;
}

function bootstrapBrokers() {
  return requiredValue(fields.bootstrapServers, 'Bootstrap servers')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);
}

function syncOperatorBrokers() {
  const selected = operatorFields.broker.value;
  const rawBootstrap = fields.bootstrapServers.value.trim();
  const brokers = rawBootstrap
    ? rawBootstrap.split(',').map((broker) => broker.trim()).filter(Boolean)
    : [];
  const options = brokers.map((broker) => {
    const option = document.createElement('option');
    option.value = broker;
    option.textContent = broker;
    return option;
  });
  operatorFields.broker.replaceChildren(...options);
  if (brokers.includes(selected)) {
    operatorFields.broker.value = selected;
  }
  overviewBroker.textContent = operatorFields.broker.value || '—';
}

function selectedBroker() {
  syncOperatorBrokers();
  return requiredValue(operatorFields.broker, 'Broker target');
}

function connectionPayload() {
  return {
    bootstrapServers: requiredValue(fields.bootstrapServers, 'Bootstrap servers')
  };
}

function messagePayload() {
  return {
    ...connectionPayload(),
    key: fields.key.value,
    message: requiredValue(fields.message, 'Message')
  };
}

function compactPayload(payload) {
  return Object.fromEntries(
    Object.entries(payload).filter(([, value]) => value !== undefined && value !== '')
  );
}

function operatorPayload(endpoint) {
  switch (endpoint) {
    case '/api/ops-status': {
      const resultLimit = numericValue(operatorFields.limit, 'Result limit');
      const partitionLimit = numericValue(operatorFields.partitionLimit, 'Partition limit');
      return {
        broker: selectedBroker(),
        brokerLimit: resultLimit,
        partitionLimit
      };
    }
    case '/api/runtime-metrics': {
      const resultLimit = numericValue(operatorFields.limit, 'Result limit');
      return {
        broker: selectedBroker(),
        counterLimit: resultLimit,
        latencyLimit: resultLimit,
        lagLimit: resultLimit
      };
    }
    case '/api/auth-status':
      return { broker: selectedBroker() };
    case '/api/failover-audit':
      return {
        broker: selectedBroker(),
        limit: numericValue(operatorFields.limit, 'Result limit')
      };
    case '/api/group-status': {
      const resultLimit = numericValue(operatorFields.limit, 'Result limit');
      const partitionLimit = numericValue(operatorFields.partitionLimit, 'Partition limit');
      return {
        ...connectionPayload(),
        groupId: requiredValue(fields.groupId, 'Consumer group'),
        topic: requiredValue(fields.topic, 'Topic'),
        memberLimit: resultLimit,
        partitionLimit
      };
    }
    case '/api/topics':
      return {
        ...connectionPayload(),
        limit: numericValue(operatorFields.limit, 'Result limit')
      };
    case '/api/topic/create':
      return {
        ...connectionPayload(),
        topic: requiredValue(fields.topic, 'Topic'),
        partitions: numericValue(fields.partitions, 'Partitions'),
        replicationFactor: numericValue(operatorFields.replicationFactor, 'Replication factor')
      };
    case '/api/topic/describe':
      return {
        ...connectionPayload(),
        topic: requiredValue(fields.topic, 'Topic'),
        includeDeleted: operatorFields.includeDeleted.checked,
        partitionLimit: numericValue(operatorFields.partitionLimit, 'Partition limit')
      };
    case '/api/topic/delete':
      return {
        ...connectionPayload(),
        topic: requiredValue(fields.topic, 'Topic')
      };
    case '/api/topic/increase':
      return {
        ...connectionPayload(),
        topic: requiredValue(fields.topic, 'Topic'),
        targetPartitionCount: numericValue(operatorFields.targetPartitionCount, 'Target partitions')
      };
    case '/api/topic/reliability': {
      const sourceTopic = requiredValue(fields.topic, 'Topic');
      return compactPayload({
        ...connectionPayload(),
        sourceTopic,
        retryTopic: operatorFields.clearRetry.checked
          ? undefined
          : operatorFields.retryTopic.value.trim(),
        deadLetterTopic: operatorFields.clearDeadLetter.checked
          ? undefined
          : operatorFields.deadLetterTopic.value.trim(),
        clearRetry: operatorFields.clearRetry.checked,
        clearDeadLetter: operatorFields.clearDeadLetter.checked
      });
    }
    case '/api/broker/offline':
      return {
        ...connectionPayload(),
        brokerId: numericValue(operatorFields.brokerId, 'Broker ID')
      };
    case '/api/delayed-produce':
      return {
        ...messagePayload(),
        topic: requiredValue(fields.topic, 'Topic'),
        deliverAtEpochMs: numericValue(operatorFields.deliverAtEpochMs, 'Delivery time')
      };
    case '/api/retry-produce': {
      return {
        ...messagePayload(),
        sourceTopic: requiredValue(fields.topic, 'Topic'),
        deliverAtEpochMs: numericValue(operatorFields.deliverAtEpochMs, 'Delivery time')
      };
    }
    case '/api/dead-letter-produce': {
      return {
        ...messagePayload(),
        sourceTopic: requiredValue(fields.topic, 'Topic')
      };
    }
    default:
      throw new Error(`Unsupported endpoint: ${endpoint}`);
  }
}

async function readJsonResponse(response) {
  const text = await response.text();
  if (!text) {
    return {};
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`HTTP ${response.status}: expected JSON response`);
  }
}

async function loadRuntimeConfig() {
  try {
    const response = await fetch('/api/config', {
      headers: { Accept: 'application/json' }
    });
    const config = await readJsonResponse(response);
    if (!response.ok || typeof config.bootstrapServers !== 'string'
        || !config.bootstrapServers.trim()) {
      throw new Error(config.error || `HTTP ${response.status}`);
    }
    fields.bootstrapServers.value = config.bootstrapServers;
    syncOperatorBrokers();
    connectionSummary.textContent = config.bootstrapServers;
    connectionState.textContent = 'Configured';
    connectionState.dataset.state = 'ready';
    overviewConfiguration.textContent = 'Ready';
  } catch (error) {
    fields.status.textContent = 'ERROR';
    fields.log.textContent = error.stack || error.message;
    connectionSummary.textContent = 'Configuration unavailable';
    connectionState.textContent = 'Configuration error';
    connectionState.dataset.state = 'error';
    overviewConfiguration.textContent = 'Error';
  }
}

async function runMainFlow() {
  fields.status.textContent = 'RUNNING';
  fields.error.hidden = true;
  if (!hasSuccessfulFlowResult) {
    fields.log.textContent = 'Sending the main flow request...';
  }
  fields.button.disabled = true;

  try {
    const response = await fetch('/api/run-flow', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode: fields.mode.value,
        bootstrapServers: requiredValue(fields.bootstrapServers, 'Bootstrap servers'),
        topic: requiredValue(fields.topic, 'Topic'),
        partitions: numericValue(fields.partitions, 'Partitions'),
        key: fields.key.value,
        message: requiredValue(fields.message, 'Message'),
        groupId: requiredValue(fields.groupId, 'Consumer group')
      })
    });
    const result = await readJsonResponse(response);
    if (!response.ok || result.ok === false) {
      throw new Error(result.error || `HTTP ${response.status}`);
    }
    fields.status.textContent = result.fallback ? 'PASS / FALLBACK' : 'PASS';
    fields.modeResult.textContent = String(result.mode || fields.mode.value).toUpperCase();
    fields.offset.textContent = String(result.offset ?? '-');
    fields.log.textContent = JSON.stringify(result, null, 2);
    hasSuccessfulFlowResult = true;
  } catch (error) {
    fields.status.textContent = 'ERROR';
    fields.error.textContent = error.message || String(error);
    fields.error.hidden = false;
    if (!hasSuccessfulFlowResult) {
      fields.modeResult.textContent = '-';
      fields.offset.textContent = '-';
      fields.log.textContent = error.stack || error.message;
    }
  } finally {
    fields.button.disabled = false;
  }
}

async function runOperator(endpoint, button) {
  const requestView = activeViewName;
  operatorFields.status.textContent = 'RUNNING';
  operatorFields.status.dataset.state = 'running';
  operatorFields.endpoint.textContent = `POST ${endpoint}`;
  operatorFields.error.hidden = true;
  overviewLastRequest.textContent = endpoint.replace('/api/', '');
  if (button) {
    button.disabled = true;
  }

  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(operatorPayload(endpoint))
    });
    const result = await readJsonResponse(response);
    if (!response.ok || result.ok === false) {
      throw new Error(result.error || `HTTP ${response.status}`);
    }
    viewResultState.set(requestView, { endpoint, result });
    if (activeViewName === requestView) {
      operatorFields.status.textContent = 'PASS';
      operatorFields.status.dataset.state = 'pass';
      renderStructuredResult(result);
    }
    return result;
  } catch (error) {
    const errorMessage = friendlyOperatorError(endpoint, error);
    const previousState = viewResultState.get(requestView);
    if (activeViewName === requestView && previousState) {
      restoreOperatorResult(requestView);
      operatorFields.status.textContent = 'ERROR';
      operatorFields.status.dataset.state = 'error';
      operatorFields.endpoint.textContent = `POST ${endpoint}`;
    }
    if (activeViewName === requestView) {
      operatorFields.status.textContent = 'ERROR';
      operatorFields.status.dataset.state = 'error';
      operatorFields.error.textContent = errorMessage;
      operatorFields.error.hidden = false;
    }
    if (activeViewName === requestView && !previousState) {
      operatorFields.structured.classList.add('empty-state');
      operatorFields.structured.textContent = errorMessage;
    }
    return null;
  } finally {
    if (button) {
      button.disabled = false;
    }
  }
}

function friendlyOperatorError(endpoint, error) {
  const message = error?.message || String(error);
  if (endpoint === '/api/broker/offline' && message.includes('INVALID_REQUEST')) {
    return 'This broker cannot be marked offline while it is the last in-sync leader. Add another online broker and retry.';
  }
  return message;
}

function confirmOperatorAction(endpoint) {
  if (endpoint === '/api/topic/delete') {
    const topic = fields.topic.value.trim();
    return !topic || window.confirm(`Delete topic ${topic}? This action cannot be undone.`);
  }
  if (endpoint === '/api/broker/offline') {
    const brokerId = operatorFields.brokerId.value.trim();
    return window.confirm(`Mark broker ${brokerId || '0'} offline? Keep another online broker available if it is the last in-sync leader.`);
  }
  return true;
}

function bindClearToggle(toggle, target) {
  const sync = () => {
    target.disabled = toggle.checked;
  };
  toggle.addEventListener('change', sync);
  sync();
}

operatorFields.deliverAtEpochMs.value = String(Date.now() + 60_000);
loadRuntimeConfig();
fields.bootstrapServers.addEventListener('input', () => {
  syncOperatorBrokers();
  connectionSummary.textContent = fields.bootstrapServers.value.trim() || 'No bootstrap server';
});
operatorFields.broker.addEventListener('change', () => {
  overviewBroker.textContent = operatorFields.broker.value || '—';
});
bindClearToggle(operatorFields.clearRetry, operatorFields.retryTopic);
bindClearToggle(operatorFields.clearDeadLetter, operatorFields.deadLetterTopic);
fields.button.addEventListener('click', runMainFlow);
operatorButtons.forEach((button) => {
  button.addEventListener('click', () => {
    if (confirmOperatorAction(button.dataset.endpoint)) {
      runOperator(button.dataset.endpoint, button);
    }
  });
});
viewButtons.forEach((button) => {
  button.addEventListener('click', () => activateView(button.dataset.viewTarget));
});
activateView('overview');

window.runMainFlow = runMainFlow;
window.runOperator = runOperator;
window.activateView = activateView;
