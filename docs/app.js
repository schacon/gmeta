// git-meta landing — animated CLI walkthrough
//
// Plays a scripted typing animation in the demo terminal once it scrolls
// into view, and mirrors the active step in the side stepper. Clicking
// "Replay" restarts the sequence from the top.

(() => {
  const term = document.getElementById('demo-body');
  const replay = document.getElementById('demo-replay');
  const demoSection = document.getElementById('demo');
  const stepEls = document.querySelectorAll('.demo-steps li');

  if (!term || !replay || !demoSection) return;

  const PROMPT_USER = '<span class="c-mut">$</span> ';
  const script = [
    {
      step: 0,
      type: 'git meta set commit:314e7f0 <k>agent:model</k> <s>claude-4.6</s>',
      out: '<mut>→ wrote string on commit:314e7f0</mut>',
    },
    {
      step: 1,
      type: 'git meta set:add path:src/metrics <k>owners</k> <s>schacon</s>',
      out: '<mut>→ added `schacon` to set (1 member) on path:src/metrics</mut>',
    },
    {
      step: 2,
      type: 'git meta get path:src/metrics',
      out:
        '<mut>owners           </mut><s>{schacon, caleb}</s>\n' +
        '<mut>review:status    </mut><s>approved</s>\n' +
        '<mut>policy:required  </mut><s>true</s>',
    },
    {
      step: 3,
      type: 'git meta set:add path:src/metrics <k>owners</k> <s>mira</s>',
      out: '<mut>→ added `mira` to set (3 members)</mut>',
    },
    {
      step: 4,
      type: 'git meta serialize && git push origin refs/meta/*',
      out:
        '<ok>→ wrote 1 metadata commit</ok>\n' +
        '<ok>→ pushed refs/meta/main  4c21a8e..f0b9012</ok>',
    },
  ];

  const COLORS = {
    k: '<span class="c-k">',
    s: '<span class="c-s">',
    mut: '<span class="c-mut">',
    ok: '<span class="c-ok">',
  };
  function colorize(s) {
    return s
      .replace(/<k>(.*?)<\/k>/g, COLORS.k + '$1</span>')
      .replace(/<s>(.*?)<\/s>/g, COLORS.s + '$1</span>')
      .replace(/<mut>(.*?)<\/mut>/g, COLORS.mut + '$1</span>')
      .replace(/<ok>(.*?)<\/ok>/g, COLORS.ok + '$1</span>');
  }

  let running = false;
  let aborter = null;

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  function setActiveStep(i) {
    stepEls.forEach((el) => el.classList.toggle('on', Number(el.dataset.step) === i));
  }

  function escapeHtml(s) {
    return s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
  }
  function stripTags(s) {
    return s.replace(/<\/?[a-z]+>/g, '');
  }

  // Type characters one at a time on top of `accum`, then swap the typed
  // text out for its colored version once the line is complete.
  async function typeLine(plain, colored, accum, speed, signal) {
    for (let i = 1; i <= plain.length; i++) {
      if (signal.cancelled) return accum;
      term.innerHTML = accum + escapeHtml(plain.slice(0, i));
      await sleep(speed + Math.random() * 10);
    }
    term.innerHTML = accum + colored + '\n';
    return accum + colored + '\n';
  }

  async function runDemo() {
    if (running) {
      aborter.cancelled = true;
      await sleep(30);
    }
    running = true;
    aborter = { cancelled: false };
    const signal = aborter;
    let accum = '';
    term.innerHTML = '';

    for (let i = 0; i < script.length; i++) {
      if (signal.cancelled) {
        running = false;
        return;
      }
      const s = script[i];
      setActiveStep(s.step);

      const plainCmd = '$ ' + stripTags(s.type);
      const coloredCmd = PROMPT_USER + colorize(s.type);
      accum = await typeLine(plainCmd, coloredCmd, accum, 14, signal);
      if (signal.cancelled) {
        running = false;
        return;
      }

      await sleep(220);
      if (signal.cancelled) {
        running = false;
        return;
      }
      const out = colorize(s.out) + '\n\n';
      accum += out;
      term.innerHTML = accum;
      await sleep(480);
    }
    accum += PROMPT_USER;
    term.innerHTML = accum;
    setActiveStep(-1);
    running = false;
  }

  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting && !running && !term.dataset.played) {
          term.dataset.played = '1';
          runDemo();
        }
      });
    },
    { threshold: 0.35 }
  );
  io.observe(demoSection);

  replay.addEventListener('click', () => {
    term.dataset.played = '1';
    runDemo();
  });
})();
