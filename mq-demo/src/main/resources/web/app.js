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
  log: document.querySelector('#log')
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
  log: document.querySelector('#operatorLog')
};

const operatorButtons = [...document.querySelectorAll('[data-endpoint]')];

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
  const brokers = bootstrapBrokers();
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
  } catch (error) {
    fields.status.textContent = 'ERROR';
    fields.log.textContent = error.stack || error.message;
  }
}

async function runMainFlow() {
  fields.status.textContent = 'RUNNING';
  fields.log.textContent = 'Sending the main flow request...';
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
  } catch (error) {
    fields.status.textContent = 'ERROR';
    fields.modeResult.textContent = '-';
    fields.offset.textContent = '-';
    fields.log.textContent = error.stack || error.message;
  } finally {
    fields.button.disabled = false;
  }
}

async function runOperator(endpoint, button) {
  operatorFields.status.textContent = 'RUNNING';
  operatorFields.status.dataset.state = 'running';
  operatorFields.endpoint.textContent = `POST ${endpoint}`;
  operatorFields.log.textContent = 'Sending operator request...';
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
    operatorFields.status.textContent = 'PASS';
    operatorFields.status.dataset.state = 'pass';
    operatorFields.log.textContent = JSON.stringify(result, null, 2);
    return result;
  } catch (error) {
    operatorFields.status.textContent = 'ERROR';
    operatorFields.status.dataset.state = 'error';
    operatorFields.log.textContent = error.stack || error.message;
    return null;
  } finally {
    if (button) {
      button.disabled = false;
    }
  }
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
fields.bootstrapServers.addEventListener('input', syncOperatorBrokers);
bindClearToggle(operatorFields.clearRetry, operatorFields.retryTopic);
bindClearToggle(operatorFields.clearDeadLetter, operatorFields.deadLetterTopic);
fields.button.addEventListener('click', runMainFlow);
operatorButtons.forEach((button) => {
  button.addEventListener('click', () => runOperator(button.dataset.endpoint, button));
});

window.runMainFlow = runMainFlow;
window.runOperator = runOperator;
