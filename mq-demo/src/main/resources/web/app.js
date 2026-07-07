const fields = {
  mode: document.querySelector('#mode'),
  bootstrapServers: document.querySelector('#bootstrapServers'),
  topic: document.querySelector('#topic'),
  message: document.querySelector('#message'),
  button: document.querySelector('#runFlow'),
  status: document.querySelector('#status'),
  modeResult: document.querySelector('#modeResult'),
  offset: document.querySelector('#offset'),
  log: document.querySelector('#log')
};

async function runMainFlow() {
  fields.status.textContent = 'RUNNING';
  fields.log.textContent = '发送主流程请求...';
  fields.button.disabled = true;

  try {
    const response = await fetch('/api/run-flow', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode: fields.mode.value,
        bootstrapServers: fields.bootstrapServers.value,
        topic: fields.topic.value,
        partitions: 1,
        key: 'web-key',
        message: fields.message.value,
        groupId: 'web-ui-group'
      })
    });
    const result = await response.json();
    if (!response.ok || !result.ok) {
      throw new Error(result.error || `HTTP ${response.status}`);
    }
    fields.status.textContent = result.fallback ? 'PASS / FALLBACK' : 'PASS';
    fields.modeResult.textContent = result.mode.toUpperCase();
    fields.offset.textContent = String(result.offset);
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

fields.button.addEventListener('click', runMainFlow);
window.runMainFlow = runMainFlow;