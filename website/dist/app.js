// The data-flow demo: a chain and a pipeline stage to describe, and the animation's pause.
const chains = {op: {name: 'OP Mainnet', id: 10}, base: {name: 'Base', id: 8453}, uni: {name: 'Unichain', id: 130}};
let selected = 'op';
let stage = 0;
let paused = matchMedia('(prefers-reduced-motion: reduce)').matches;
const descriptions = [
  () => `Connect to ${chains[selected].name} peers and receive sequencer-signed blocks from the gossip network.`,
  () => 'Check sequencer signatures, block hashes and receipt roots. With L1 on, follow the safe and finalized heads.',
  () => 'Keep committed blocks in the archive, or seal finalized history into chunks in object storage.',
  () => 'Serve history, then live blocks, over gRPC. Read blocks, transactions, receipts and logs with Arrow Flight.',
];

function render() {
  document.querySelector('#stage-description').textContent = descriptions[stage]();
  document.querySelector('#chain-id b').textContent = chains[selected].id;
  document.querySelectorAll('.chain').forEach(button => {
    const active = button.dataset.chain === selected;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active);
  });
  document.querySelectorAll('.stage').forEach(button => {
    const active = Number(button.dataset.stage) === stage;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active);
  });
}

function setPaused() {
  const svg = document.querySelector('.wires');
  const button = document.querySelector('#pause');
  if (paused) svg.pauseAnimations(); else svg.unpauseAnimations();
  button.textContent = paused ? '▷ Play' : 'Ⅱ Pause';
  button.setAttribute('aria-label', paused ? 'Play the data flow animation' : 'Pause the data flow animation');
}

document.querySelectorAll('.chain').forEach(button => button.addEventListener('click', () => {
  selected = button.dataset.chain;
  render();
}));
document.querySelectorAll('.stage').forEach(button => button.addEventListener('click', () => {
  stage = Number(button.dataset.stage);
  render();
}));
document.querySelector('#pause').addEventListener('click', () => {
  paused = !paused;
  setPaused();
});

// Copy buttons: the command into the clipboard, or selected for a manual copy.
document.querySelectorAll('[data-copy]').forEach(button => {
  const label = button.textContent;
  let reset;
  button.addEventListener('click', async () => {
    const command = document.getElementById(button.dataset.copy);
    const status = document.getElementById(button.dataset.status);
    try {
      await navigator.clipboard.writeText(command.textContent.trim());
      button.textContent = 'Copied';
      button.classList.add('copied');
      status.textContent = 'Copied. Paste it into your server\'s terminal.';
    } catch {
      const range = document.createRange();
      range.selectNodeContents(command);
      getSelection().removeAllRanges();
      getSelection().addRange(range);
      status.textContent = 'Selected. Press Ctrl+C or ⌘C to copy.';
    }
    clearTimeout(reset);
    reset = setTimeout(() => {
      button.textContent = label;
      button.classList.remove('copied');
      status.textContent = '';
    }, 2500);
  });
});

render();
setPaused();
