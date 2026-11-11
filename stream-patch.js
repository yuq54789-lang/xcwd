/*
 * EPhone 流式输出补丁 (stream-patch.js)
 * ------------------------------------------------------------
 * 外挂文件，不修改 script.js。
 * 开关打开后：对话类请求（OpenAI 格式 /chat/completions、Gemini :generateContent）
 * 改用流式传输，在后台把数据流收完，再拼成和非流式完全一样的 JSON 交还给原程序。
 * 好处：长回复不容易被反代/网关的超时掐断。界面显示行为不变（不逐字显示）。
 * 关闭开关 = 完全不干预。流式出错 = 和原来一样报错（429、网络问题等），不做静默回退。
 * 开关位置：API设置 -> 主 API -> 流式输出
 */
(function () {
  'use strict';
  if (window.__ephoneStreamPatch) return;
  window.__ephoneStreamPatch = true;

  var KEY = 'ephone_stream_enabled';
  function isOn() {
    try { return localStorage.getItem(KEY) === '1'; } catch (e) { return false; }
  }
  function setOn(v) {
    try { localStorage.setItem(KEY, v ? '1' : '0'); } catch (e) {}
  }

  var origFetch = window.fetch.bind(window);

  // ---------- 判断是不是对话请求 ----------
  function classify(input, init) {
    try {
      if (!init || typeof init.body !== 'string') return null;
      if (String(init.method || 'GET').toUpperCase() !== 'POST') return null;
      var raw = typeof input === 'string' ? input : (input instanceof URL ? input.href : null);
      if (!raw) return null;
      var u = new URL(raw, location.href);
      if (/\/chat\/completions\/?$/.test(u.pathname)) return { kind: 'openai', url: u };
      if (/:generateContent$/.test(u.pathname)) return { kind: 'gemini', url: u };
    } catch (e) {}
    return null;
  }

  // ---------- SSE 读取 ----------
  async function readSSE(res, onData) {
    var reader = res.body.getReader();
    var decoder = new TextDecoder('utf-8');
    var buf = '';
    var got = false;
    try {
      while (true) {
        var r = await reader.read();
        if (r.done) break;
        buf += decoder.decode(r.value, { stream: true });
        var lines = buf.split(/\r?\n/);
        buf = lines.pop();
        for (var i = 0; i < lines.length; i++) {
          var line = lines[i].trim();
          if (line.indexOf('data:') !== 0) continue;
          var data = line.slice(5).trim();
          if (!data) continue;
          if (data === '[DONE]') return got;
          var obj;
          try { obj = JSON.parse(data); } catch (e) { continue; }
          got = true;
          onData(obj);
        }
      }
      buf += decoder.decode();
      var tail = buf.trim();
      if (tail.indexOf('data:') === 0) {
        var d = tail.slice(5).trim();
        if (d && d !== '[DONE]') {
          try { onData(JSON.parse(d)); got = true; } catch (e) {}
        }
      }
    } finally {
      try { reader.releaseLock(); } catch (e) {}
    }
    return got;
  }

  function jsonResponse(obj, status) {
    return new Response(JSON.stringify(obj), {
      status: status || 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  function errStatus(e) {
    var c = e && (e.code || e.status);
    return (typeof c === 'number' && c >= 400 && c < 600) ? c : 500;
  }

  function isAborted(init) {
    return !!(init && init.signal && init.signal.aborted);
  }

  // ---------- OpenAI 格式 ----------
  async function runOpenAI(input, init, info) {
    var body;
    try { body = JSON.parse(init.body); } catch (e) { return null; }
    if (!body || !Array.isArray(body.messages) || body.stream === true) return null;
    body.stream = true;

    var res = await origFetch(input, Object.assign({}, init, { body: JSON.stringify(body) }));
    var ct = res.headers.get('content-type') || '';
    if (!res.ok || !res.body || ct.indexOf('text/event-stream') === -1) return res;

    var content = '', reasoning = '', finish = null, usage = null, model = null, id = null, err = null;
    var got = await readSSE(res, function (o) {
      if (o.error) { err = o.error; return; }
      if (o.id) id = o.id;
      if (o.model) model = o.model;
      if (o.usage) usage = o.usage;
      var c = o.choices && o.choices[0];
      if (!c) return;
      if (c.delta) {
        if (typeof c.delta.content === 'string') content += c.delta.content;
        if (typeof c.delta.reasoning_content === 'string') reasoning += c.delta.reasoning_content;
      } else if (c.message && typeof c.message.content === 'string') {
        content += c.message.content;
      }
      if (c.finish_reason) finish = c.finish_reason;
    });

    if (err && !content) return jsonResponse({ error: typeof err === 'object' ? err : { message: String(err) } }, errStatus(err));
    var msg = { role: 'assistant', content: content };
    if (reasoning) msg.reasoning_content = reasoning;
    var out = { choices: [{ index: 0, message: msg, finish_reason: finish || 'stop' }] };
    if (id) out.id = id;
    if (model) out.model = model;
    if (usage) out.usage = usage;
    out.object = 'chat.completion';
    return jsonResponse(out);
  }

  // ---------- Gemini 官方格式 ----------
  async function runGemini(input, init, info) {
    var u = new URL(info.url.href);
    u.pathname = u.pathname.replace(/:generateContent$/, ':streamGenerateContent');
    u.searchParams.set('alt', 'sse');

    var res = await origFetch(u.href, init);
    var ct = res.headers.get('content-type') || '';
    if (!res.ok || !res.body || ct.indexOf('text/event-stream') === -1) return res;

    var parts = [], cand = null, usage = null, feedback = null, modelVersion = null, err = null;
    await readSSE(res, function (o) {
      if (o.error) { err = o.error; return; }
      if (o.usageMetadata) usage = o.usageMetadata;
      if (o.promptFeedback) feedback = o.promptFeedback;
      if (o.modelVersion) modelVersion = o.modelVersion;
      var c = o.candidates && o.candidates[0];
      if (!c) return;
      cand = Object.assign(cand || {}, c);
      var ps = c.content && c.content.parts;
      if (!ps) return;
      ps.forEach(function (p) {
        var last = parts[parts.length - 1];
        if (typeof p.text === 'string' && last && typeof last.text === 'string' &&
            !!last.thought === !!p.thought) {
          last.text += p.text;
          if (p.thoughtSignature) last.thoughtSignature = p.thoughtSignature;
        } else {
          parts.push(JSON.parse(JSON.stringify(p)));
        }
      });
    });

    if (err && !parts.length) return jsonResponse({ error: typeof err === 'object' ? err : { message: String(err) } }, errStatus(err));
    var out = {};
    if (cand) {
      cand.content = { role: 'model', parts: parts };
      out.candidates = [cand];
    }
    if (usage) out.usageMetadata = usage;
    if (feedback) out.promptFeedback = feedback;
    if (modelVersion) out.modelVersion = modelVersion;
    return jsonResponse(out);
  }

  // ---------- 接管 fetch ----------
  window.fetch = async function (input, init) {
    var info = isOn() ? classify(input, init) : null;
    if (!info) return origFetch(input, init);
    // 不做静默回退：流式出错就原样报错，和原来的报错提示一致
    // (HTTP 错误如 429/401 原样透传；网络中断会抛出网络错误，界面显示"网络请求失败: ...")
    var r = info.kind === 'gemini'
      ? await runGemini(input, init, info)
      : await runOpenAI(input, init, info);
    if (r) return r;
    return origFetch(input, init);
  };

  // ---------- 设置页开关 ----------
  function injectToggle() {
    if (document.getElementById('stream-output-switch')) return true;
    var sel = document.getElementById('model-select');
    var item = sel && sel.closest('.settings-item');
    if (!item) return false;
    var row = document.createElement('div');
    row.className = 'settings-item';
    row.id = 'stream-output-item';
    row.innerHTML =
      '<label>流式输出</label>' +
      '<label class="toggle-switch">' +
      '<input type="checkbox" id="stream-output-switch"><span class="slider"></span>' +
      '</label>';
    item.parentNode.insertBefore(row, item.nextSibling);
    var cb = row.querySelector('input');
    cb.checked = isOn();
    cb.addEventListener('change', function () { setOn(cb.checked); });
    return true;
  }

  function init() {
    if (injectToggle()) return;
    var tries = 0;
    var t = setInterval(function () {
      if (injectToggle() || ++tries > 20) clearInterval(t);
    }, 500);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
