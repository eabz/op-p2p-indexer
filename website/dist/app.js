const chains={op:{name:'OP Mainnet',id:10},base:{name:'Base',id:8453},uni:{name:'Unichain',id:130}};
let selected='op',stage=0,paused=matchMedia('(prefers-reduced-motion: reduce)').matches;
const descriptions=[()=>`Connect to ${chains[selected].name} peers. Receive sequencer-signed blocks directly from the gossip network.`,()=>`Validate sequencer signatures and receipt roots. With L1 tracking enabled, follow safe and finalized heads.`,()=>`Promote committed blocks into the local archive. Fleet exporters seal finalized history into immutable chunks.`,()=>`Serve history, then live data over gRPC. Read columnar blocks, transactions, receipts and logs with Arrow Flight.`];
function render(){document.querySelector('#stage-description').textContent=descriptions[stage]();document.querySelector('#chain-id b').textContent=chains[selected].id;document.querySelectorAll('.chain').forEach(b=>{const active=b.dataset.chain===selected;b.classList.toggle('active',active);b.setAttribute('aria-pressed',active)});document.querySelectorAll('.stage').forEach(b=>{const active=Number(b.dataset.stage)===stage;b.classList.toggle('active',active);b.setAttribute('aria-pressed',active)})}
document.querySelectorAll('.chain').forEach(b=>b.addEventListener('click',()=>{selected=b.dataset.chain;render()}));document.querySelectorAll('.stage').forEach(b=>b.addEventListener('click',()=>{stage=Number(b.dataset.stage);render()}));
function setPaused(){const svg=document.querySelector('.wires');paused?svg.pauseAnimations():svg.unpauseAnimations();document.querySelector('#pause').textContent=paused?'▷ Play':'Ⅱ Pause';document.querySelector('#pause').setAttribute('aria-label',paused?'Play data flow animation':'Pause data flow animation')}
document.querySelector('#pause').addEventListener('click',()=>{paused=!paused;setPaused()});setPaused();
document.querySelectorAll('[data-copy]').forEach(button => {
  const label = button.textContent;
  let reset;
  button.addEventListener('click', async () => {
    const status = document.getElementById(button.getAttribute('aria-describedby'));
    const command = document.getElementById(button.dataset.copy);
    try {
      await navigator.clipboard.writeText(command.textContent.trim());
      status.textContent = 'Copied. Paste the command into your server terminal.';
      button.textContent = 'Copied!';
      clearTimeout(reset);
      reset = setTimeout(() => { button.textContent = label; }, 2500);
    } catch {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(command);
      selection.removeAllRanges();
      selection.addRange(range);
      command.scrollIntoView({block: 'center'});
      status.textContent = 'Command selected. Press Ctrl+C or ⌘C to copy.';
    }
  });
});
render();
