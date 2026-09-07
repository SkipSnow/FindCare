/* Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
   Licensed under the FindCare Evaluation License (FEL-1.0).

   ClientRouter.js — the one and only client-side router.

   Contract (per brain/BusinessArtifacts/architecture/FrontEnd/FrontEndArchitecture.pptx):

   render({target, append, popup, content})
     target  : string — name of a frame (Header, Footer, LeftPanel,
               RightPanel, UserMessage, UserPromptAndControl, MainWindow)
     append  : boolean — true: append content; false: replace.
     content : string — HTML fragment to inject.

     Popups are React's. The overlay, its chrome and its close control used
     to be built here, which is display authored outside React and is what
     made the parent the only thing able to print the session window.

   getStreamedPayloads({op, payload, onEvent, onFinal, onError})
     Posts to SharedServices /gate as op + payload, reads the NDJSON
     stream, dispatches every {kind, data} envelope to subscribers and
     to the caller's onEvent. Calls onFinal once when {kind:'final'}
     arrives. Calls onError on any failure. Use for ops where the server
     emits multiple payloads over time (utterances, tool dispatch).

   getFullPayload({op, payload})
     Posts to SharedServices /gate as op + payload, reads a single JSON
     response body, returns it as a Promise. Use for ops where the
     server returns one complete payload (peer_urls, peer_health,
     session, verify_token, transfer_to_findcare).

   subscribe(kind, handler)
     Subscribes a handler to every stream event with this kind. Used by
     React widgets to react to tool output without each subscribing to
     /gate independently. Returns an unsubscribe function.

   The wrapper exposes window.ClientRouter as the single API surface.
   Cross-iframe callers (React widget in the chat iframe) post a
   message of type 'router:render' or 'router:makeCall' to the wrapper;
   ClientRouter receives the postMessage and acts on it.
*/
(function () {

  function _getEnvServiceUrls() {
    return (window._envServiceUrls && window._envServiceUrls.sharedservices)
      ? window._envServiceUrls
      : { sharedservices: 'https://localhost:8002' };
  }

  function _sharedGateUrl() {
    var urls = _getEnvServiceUrls();
    return urls.sharedservices;
  }

  function _frameElement(target) {
    var el = document.getElementById('frame_' + target);
    return el;
  }

  var _subscribers = {};

  // Full signed SessionToken (wire object). Carried across every /gate
  // call so SS validates and hydrates the same user_object each time.
  // Replaces the earlier bearer-only GUID pattern: /gate now verifies
  // the signature on every non-trivial op, not just the tail-32 lookup.
  var _sessionToken = null;

  // The GUID a session is known by, kept for the life of the tab. The
  // token is per hop and dies with the page; the GUID is per session and
  // outlives a reload, which is what lets a reload resume instead of
  // starting a second session and orphaning the first.
  var _GUID_KEY = 'ch_session_guid';

  function _rememberGuid(st) {
    try {
      if (st && typeof st.token === 'string' && st.token.length >= 32) {
        sessionStorage.setItem(_GUID_KEY, st.token.slice(-32));
      }
    } catch (e) { /* storage unavailable is not a failure worth stopping for */ }
  }

  function _rememberedGuid() {
    try { return sessionStorage.getItem(_GUID_KEY) || ''; } catch (e) { return ''; }
  }

  function _captureSessionToken(evt) {
    if (!evt || typeof evt !== 'object') return;
    var st = evt.session_token;
    if (!st || typeof st !== 'object') return;
    if (typeof st.token !== 'string' || st.token.length < 32) return;
    _sessionToken = st;
    _rememberGuid(st);
  }

  // bootstrap — fetch a freshly-minted SessionToken from SS's /auth/issue
  // endpoint. Called by the wrapper on page load so ClientRouter holds a
  // valid signed token before the first non-trivial /gate call fires.
  // /auth/issue itself is un-authenticated; every downstream /gate call
  // is validated using the token this returns.
  function bootstrap() {
    var url = _sharedGateUrl() + '/auth/issue';
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // The form factor is told to the session on the call that makes
      // it, and on no other. Only the browser knows it -- a narrow
      // window on a computer is a phone as far as the arrangement is
      // concerned -- and it does not change while the session lives.
      // A session we already have is offered back. Having one means we
      // do not need a new one: the server stamps a fresh token against
      // it rather than building a second session beside it.
      body: JSON.stringify({
        session_guid: _rememberedGuid(),
        form_factor: _isPhone() ? 'phone' : 'desktop',
      }),
    }).then(function (resp) {
      if (!resp.ok) {
        throw new Error('ClientRouter.bootstrap: /auth/issue failed HTTP ' + resp.status);
      }
      return resp.json();
    }).then(function (st) {
      _sessionToken = st;
      _rememberGuid(st);
      return st;
    });
  }

  // The website's own build, from the file the build stamped into it. The
  // site deploys on its own cadence, so this legitimately differs from the
  // servers and the session shows both rather than assuming one number.
  function buildIdentity() {
    return (window.CH_BUILD && typeof window.CH_BUILD === 'object')
      ? window.CH_BUILD : {};
  }

  function _bindActions(root) {
    if (!root || !root.querySelectorAll) return;
    var nodes = root.querySelectorAll('[data-router-action]');
    for (var i = 0; i < nodes.length; i++) {
      (function (el) {
        var action = el.getAttribute('data-router-action');
        var tag = (el.tagName || '').toLowerCase();
        var listen = (tag === 'form') ? 'submit'
          : (tag === 'input' || tag === 'select' || tag === 'textarea') ? 'change'
          : 'click';
        el.addEventListener(listen, function (ev) {
          ev.preventDefault();
          // Moving the strip is the wrapper's own work -- it is where
          // the panels are. Sending it to React and having React ask
          // for it back would be a round trip for a scroll.
          if (action === 'panel_left')  { _step(-1); return; }
          if (action === 'panel_right') { _step(1);  return; }
          var data = {};
          var attrs = el.attributes;
          for (var j = 0; j < attrs.length; j++) {
            var a = attrs[j];
            if (a.name.indexOf('data-') === 0 && a.name !== 'data-router-action') {
              // Convert hyphens to underscores so widgets read keys
              // verbatim — e.g. data-trial-idx is accessible as
              // msg.data.trial_idx (HTML attribute names are
              // hyphenated; JS object property convention is _).
              data[a.name.substring(5).replace(/-/g, '_')] = a.value;
            }
          }
          if (tag === 'form') {
            var inputs = el.querySelectorAll('input[name], select[name], textarea[name]');
            for (var k = 0; k < inputs.length; k++) {
              data[inputs[k].name] = inputs[k].value;
            }
          } else if (tag === 'input' || tag === 'select' || tag === 'textarea') {
            data.value = el.value;
          }
          var iframe = document.querySelector('iframe[data-frame="MainWindow"]');
          if (iframe && iframe.contentWindow) {
            iframe.contentWindow.postMessage({
              type: 'router:action',
              action: action,
              data: data,
            }, '*');
          }
        });
      })(nodes[i]);
    }
    // Drag sources: any draggable element with data-drag-payload writes
    // its payload to dataTransfer on dragstart. The payload is whatever
    // the widget set (typically an NPI or other id the drop target needs).
    var dragSrc = root.querySelectorAll('[draggable="true"][data-drag-payload]');
    for (var s = 0; s < dragSrc.length; s++) {
      (function (el) {
        el.addEventListener('dragstart', function (ev) {
          ev.dataTransfer.setData('text/plain', el.getAttribute('data-drag-payload') || '');
          ev.dataTransfer.effectAllowed = 'move';
          el.style.opacity = '0.5';
        });
        el.addEventListener('dragend', function () { el.style.opacity = ''; });
      })(dragSrc[s]);
    }
    // Drop targets: any element with data-router-drop-action accepts
    // drops and fires a router:action with the dropped payload at data.dropped.
    // Receiving widget then invokes getStreamedPayloads to mutate state server-side.
    var dropTgt = root.querySelectorAll('[data-router-drop-action]');
    for (var d = 0; d < dropTgt.length; d++) {
      (function (el) {
        var action = el.getAttribute('data-router-drop-action');
        el.addEventListener('dragover', function (ev) {
          ev.preventDefault();
          ev.dataTransfer.dropEffect = 'move';
        });
        el.addEventListener('drop', function (ev) {
          ev.preventDefault();
          var dropped = ev.dataTransfer.getData('text/plain');
          if (!dropped) return;
          var iframe = document.querySelector('iframe[data-frame="MainWindow"]');
          if (iframe && iframe.contentWindow) {
            iframe.contentWindow.postMessage({
              type: 'router:action',
              action: action,
              data: { dropped: dropped },
            }, '*');
          }
        });
      })(dropTgt[d]);
    }
  }

  function render(args) {
    var target = args && args.target;
    if (!target) return;
    var content = (args && args.content) || '';
    var append = !!(args && args.append);
    var sink = _frameElement(target);
    if (!sink) {
      // Silently dropping the content makes a page that never received it
      // indistinguishable from a feature that does not work. A frame the
      // page does not declare is a fault in the page, and it says so.
      console.error('ClientRouter.render: no frame_' + target +
                    ' in this page; content was not delivered');
      return;
    }
    if (append) {
      var tmp = document.createElement('div');
      tmp.innerHTML = content;
      while (tmp.firstChild) sink.appendChild(tmp.firstChild);
    } else {
      sink.innerHTML = content;
    }
    _bindActions(sink);
  }

  // merge — fill a named region inside a frame's scaffold without
  // disturbing the rest of the frame. The scaffold (painted by render)
  // defines the region ids; merge replaces the innerHTML of the element
  // whose id matches `region`. No-op when the region element is absent
  // (scaffold not yet painted) so widgets can subscribe defensively.
  function merge(args) {
    var target = args && args.target;
    var region = args && args.region;
    if (!target || !region) return;
    var content = (args && args.content) || '';
    var frame = _frameElement(target);
    if (!frame) return;
    var sink = frame.querySelector('#' + region);
    if (!sink) {
      console.error('ClientRouter.merge: frame_' + target +
                    ' has no region #' + region + '; content was not delivered');
      return;
    }
    sink.innerHTML = content;
    _bindActions(sink);
  }

  // A window is moved by dragging its header. Position is the person's
  // to set, so once they move one it stops being centred. This composes
  // nothing -- it reads a pointer and writes two coordinates.
  (function () {
    var held = null, dx = 0, dy = 0;
    document.addEventListener('pointerdown', function (ev) {
      var handle = ev.target && ev.target.closest &&
                   ev.target.closest('.ch-popup-drag');
      if (!handle) return;
      var win = handle.closest('.ch-popup');
      if (!win) return;
      var box = win.getBoundingClientRect();
      win.classList.add('ch-moved');
      win.style.left = box.left + 'px';
      win.style.top = box.top + 'px';
      held = win; dx = ev.clientX - box.left; dy = ev.clientY - box.top;
      ev.preventDefault();
    });
    document.addEventListener('pointermove', function (ev) {
      if (!held) return;
      held.style.left = (ev.clientX - dx) + 'px';
      held.style.top = (ev.clientY - dy) + 'px';
    });
    document.addEventListener('pointerup', function () { held = null; });
  })();

  function _dispatchEvent(evt, caller) {
    if (!evt || typeof evt !== 'object') return;
    // Capture the freshly-restamped SessionToken from any stream event
    // that carries one, so the next /gate call sends the current signed
    // token (with an advanced nonce) rather than a stale copy.
    _captureSessionToken(evt);
    var kind = evt.kind || '';
    var handlers = _subscribers[kind] || [];
    for (var i = 0; i < handlers.length; i++) {
      try { handlers[i](evt.data || {}, evt); } catch (_) {}
    }
    if (caller && typeof caller.onEvent === 'function') {
      try { caller.onEvent(evt); } catch (_) {}
    }
  }

  function getStreamedPayloads(args) {
    var op = args && args.op;
    if (!op) return Promise.reject(new Error('getStreamedPayloads: op is required'));
    var payload = (args && args.payload) || {};
    var url = _sharedGateUrl() + '/gate';
    // Thread the session GUID so SS reloads the same user_object on
    // every call instead of minting a fresh one (which would lose the
    // session_conversation_history that powers the SharedServices
    // splash + UM transcript window).
    var body = { op: op, payload: payload };
    if (_sessionToken) body.session_token = _sessionToken;
    return fetch(url, {
      method: 'POST',
      // No credentials:'include' on the fetch. HF Spaces' edge proxy
      // strips Access-Control-Allow-Credentials from the OPTIONS
      // preflight response, which blocks credentialed cross-origin
      // requests entirely. We thread the signed session token via the
      // body-level `session_token` field instead — SS verifies it at
      // app.py /gate before dispatching any non-trivial op. No cookies
      // anywhere in the architecture.
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/x-ndjson',
      },
      body: JSON.stringify(body),
    }).then(function (resp) {
      if (!resp.ok || !resp.body) {
        // The failure's mode is carried, not composed. It rides on the
        // error so the panel that renders it can select its wording from
        // what the server named.
        return resp.json().catch(function () { return {}; }).then(function (b) {
          var err = new Error('ClientRouter.getStreamedPayloads: gate failed HTTP ' + resp.status + ' for op=' + op);
          err.mode = (b && b.mode) || '';
          if (args && typeof args.onError === 'function') args.onError(err);
          throw err;
        });
      }
      var reader = resp.body.getReader();
      var decoder = new TextDecoder();
      var buffer = '';
      function pump() {
        return reader.read().then(function (r) {
          if (r.done) {
            if (buffer.trim()) {
              try { _dispatchEvent(JSON.parse(buffer.trim()), args); } catch (_) {}
            }
            if (args && typeof args.onFinal === 'function') args.onFinal();
            return;
          }
          buffer += decoder.decode(r.value, { stream: true });
          var nl;
          while ((nl = buffer.indexOf('\n')) >= 0) {
            var line = buffer.substring(0, nl).trim();
            buffer = buffer.substring(nl + 1);
            if (!line) continue;
            try { _dispatchEvent(JSON.parse(line), args); }
            catch (_) {}
          }
          return pump();
        });
      }
      return pump();
    }).catch(function (err) {
      if (args && typeof args.onError === 'function') args.onError(err);
      throw err;
    });
  }

  // getFullPayload — for /gate ops in _TRIVIAL_GATE_OPS (peer_urls, peer_health,
  // session, verify_token, transfer_to_findcare) that return plain JSON
  // instead of NDJSON. Same credentials discipline as getStreamedPayloads so the
  // session cookie threads through. Returns a Promise of the parsed JSON
  // body. Use this for the bootstrap peer_urls fetch and any other
  // request/response op that does not need streaming.
  function getFullPayload(args) {
    var op = args && args.op;
    if (!op) return Promise.reject(new Error('getFullPayload: op is required'));
    var payload = (args && args.payload) || {};
    var url = _sharedGateUrl() + '/gate';
    var body = { op: op, payload: payload };
    // Trivial ops are pre-authentication utilities (peer_urls,
    // peer_health, etc.) and do not require the signed token. Include
    // it when we already hold one so downstream logging/audit can see
    // the caller's session, but never require it here.
    if (_sessionToken) body.session_token = _sessionToken;
    return fetch(url, {
      method: 'POST',
      // No credentials:'include' for the same reason as getStreamedPayloads —
      // see comment there. Trivial ops carry the token when we have
      // one but do not require it.
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then(function (resp) {
      if (!resp.ok) {
        throw new Error('ClientRouter.getFullPayload: gate failed HTTP ' + resp.status + ' for op=' + op);
      }
      return resp.json();
    });
  }

  function subscribe(kind, handler) {
    if (!kind || typeof handler !== 'function') return function () {};
    if (!_subscribers[kind]) _subscribers[kind] = [];
    _subscribers[kind].push(handler);
    return function () {
      var arr = _subscribers[kind] || [];
      var idx = arr.indexOf(handler);
      if (idx >= 0) arr.splice(idx, 1);
    };
  }

  window.addEventListener('message', function (event) {
    var msg = event.data;
    if (!msg || typeof msg !== 'object') return;
    if (msg.type === 'router:render') {
      render({
        target: msg.target,
        append: msg.append,
        content: msg.content,
      });
    } else if (msg.type === 'router:merge') {
      merge({
        target: msg.target,
        region: msg.region,
        content: msg.content,
      });
    } else if (msg.type === 'router:makeCall') {
      getStreamedPayloads({
        op: msg.op,
        payload: msg.payload,
        onEvent: function (evt) {
          var iframe = document.querySelector('iframe[data-frame="MainWindow"]');
          if (iframe && iframe.contentWindow) {
            iframe.contentWindow.postMessage({
              type: 'router:event',
              call_id: msg.call_id,
              evt: evt,
            }, '*');
          }
        },
        onFinal: function () {
          var iframe = document.querySelector('iframe[data-frame="MainWindow"]');
          if (iframe && iframe.contentWindow) {
            iframe.contentWindow.postMessage({
              type: 'router:final',
              call_id: msg.call_id,
            }, '*');
          }
        },
        onError: function (err) {
          var iframe = document.querySelector('iframe[data-frame="MainWindow"]');
          if (iframe && iframe.contentWindow) {
            iframe.contentWindow.postMessage({
              type: 'router:error',
              call_id: msg.call_id,
              error: (err && err.message) || String(err),
              mode: (err && err.mode) || '',
            }, '*');
          }
        },
      });
    } else if (msg.type === 'router:subscribe-broadcast') {
      var unsub = subscribe(msg.kind, function (data, evt) {
        var iframe = document.querySelector('iframe[data-frame="MainWindow"]');
        if (iframe && iframe.contentWindow) {
          iframe.contentWindow.postMessage({
            type: 'router:event-broadcast',
            kind: msg.kind,
            data: data,
            evt: evt,
          }, '*');
        }
      });
      window['__unsub_' + msg.kind] = unsub;
    } else if (msg.type === 'router:ask-build') {
      var frame = document.querySelector('iframe[data-frame="MainWindow"]');
      if (frame && frame.contentWindow) {
        frame.contentWindow.postMessage(
          { type: 'router:website-build', build: buildIdentity() }, '*');
      }
    } else if (msg.type === 'router:download') {
      // Ask /gate for a file and let the browser save it. One entrance:
      // the download is an op like any other. The print dialogue this
      // replaces could not produce a PDF on a phone at all.
      (function () {
        var body = { op: msg.op, payload: msg.payload || {} };
        if (_sessionToken) body.session_token = _sessionToken;
        fetch(_sharedGateUrl() + '/gate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        }).then(function (resp) {
          if (!resp.ok) throw new Error('download failed HTTP ' + resp.status);
          return resp.blob();
        }).then(function (blob) {
          var url = URL.createObjectURL(blob);
          var a = document.createElement('a');
          a.href = url;
          a.download = msg.filename || 'download';
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
        }).catch(function (err) {
          console.error('ClientRouter.download: ' + err);
        });
      })();
    } else if (msg.type === 'router:exec') {
      try { new Function(String(msg.code || ''))(); } catch (_) {}
    }
  });

  function getSessionToken() {
    return _sessionToken;
  }

  function getSessionGuid() {
    if (!_sessionToken || typeof _sessionToken.token !== 'string') return null;
    return _sessionToken.token.slice(-32);
  }

  // Serialize a /gate request body with the current session token
  // attached. Exposed on window so the Playwright smoke suite can build
  // valid /gate posts from page.evaluate blocks without duplicating the
  // token-attach logic. Callers pass the op + payload; the helper
  // returns a ready-to-POST string.
  function gateBody(args) {
    var body = {
      op: (args && args.op),
      payload: (args && args.payload) || {},
    };
    if (args && args.intent) body.intent = args.intent;
    if (_sessionToken) body.session_token = _sessionToken;
    return JSON.stringify(body);
  }

  // ── Moving between panels on a phone ─────────────────────────────
  // The strip is one row of panels wider than the screen. This decides
  // where it sits and which arrows can be offered; React authors what an
  // arrow looks like and this authors no content at all. Every length is
  // read from the elements themselves, never written here.

  function _row() { return document.querySelector('.content-row'); }

  // A panel is blank when it shows nothing. :empty cannot say that: a
  // widget lays an empty container into a panel and the selector sees a
  // child node, so a panel with nothing in it counted as a panel, took a
  // share of the screen, and offered an arrow onto a blank screen.
  function _markBlankPanels() {
    var row = _row(); if (!row) return;
    var sides = row.querySelectorAll('aside.side-panel');
    for (var i = 0; i < sides.length; i++) {
      var el = sides[i];
      var hasText = (el.innerText || '').trim().length > 0;
      var hasThing = !!el.querySelector('img, svg, canvas, input, button, table');
      el.classList.toggle('ch-blank', !(hasText || hasThing));
    }
  }

  function _panels() {
    var row = _row();
    if (!row) return [];
    _markBlankPanels();
    return Array.prototype.filter.call(row.children, function (k) {
      var st = getComputedStyle(k);
      return st.display !== 'none' && k.getBoundingClientRect().width > 0;
    });
  }

  // Which panel the screen is looking at: the one whose span covers the
  // middle of the viewport.
  // Where a panel sits along the row, in the row's own terms. offsetLeft
  // is measured against whatever the offset parent happens to be -- the
  // body here, because the row is not positioned -- so it carried the
  // page padding and every scroll landed a few pixels short of the panel.
  function _panelLeft(row, el) {
    return Math.round(el.getBoundingClientRect().left
                      - row.getBoundingClientRect().left + row.scrollLeft);
  }

  function _currentPanelIndex() {
    var row = _row(); if (!row) return 0;
    var mid = row.scrollLeft + row.clientWidth / 2;
    var panels = _panels(), best = 0, bestGap = Infinity;
    for (var i = 0; i < panels.length; i++) {
      var left = _panelLeft(row, panels[i]);
      var gap = Math.abs(left + panels[i].getBoundingClientRect().width / 2 - mid);
      if (gap < bestGap) { bestGap = gap; best = i; }
    }
    return best;
  }

  function _refreshArrowState() {
    var panels = _panels(), i = _currentPanelIndex();
    document.body.classList.toggle('ch-can-left', i > 0);
    document.body.classList.toggle('ch-can-right', i < panels.length - 1);
  }

  function _goToPanel(index) {
    var row = _row(); if (!row) return;
    var panels = _panels();
    if (index < 0 || index >= panels.length) return;
    row.scrollTo({ left: _panelLeft(row, panels[index]), behavior: 'smooth' });
    document.body.classList.add('ch-panel-focus');
    setTimeout(_refreshArrowState, 400);
  }

  function _step(direction) { _goToPanel(_currentPanelIndex() + direction); }

  function _isPhone() { return window.matchMedia('(max-width: 45em)').matches; }

  function _wirePanelNav() {
    var row = _row();
    if (!row || row.__chPanelNav) return;
    row.__chPanelNav = true;

    // A scroll that carries more than a fifth of the way into a
    // neighbour is a move to that neighbour, so it completes rather than
    // leaving the person between two panels.
    var settle = null;
    row.addEventListener('scroll', function () {
      _refreshArrowState();
      if (!_isPhone()) return;
      if (settle) clearTimeout(settle);
      settle = setTimeout(function () {
        var panels = _panels(); if (!panels.length) return;
        var i = _currentPanelIndex();
        var here = panels[i];
        var past = row.scrollLeft - _panelLeft(row, here);
        var share = past / here.getBoundingClientRect().width;
        if (share > 0.2 && i < panels.length - 1) _goToPanel(i + 1);
        else if (share < -0.2 && i > 0) _goToPanel(i - 1);
        else _goToPanel(i);
      }, 140);
    }, { passive: true });

    // A double tap on one side of the screen is that side's arrow.
    var lastTap = 0;
    row.addEventListener('click', function (ev) {
      if (!_isPhone()) return;
      var now = Date.now();
      var quick = now - lastTap < 350;
      lastTap = now;
      if (!quick) return;
      _step(ev.clientX < window.innerWidth / 2 ? -1 : 1);
    });

    _refreshArrowState();
  }

  window.addEventListener('resize', _refreshArrowState);
  document.addEventListener('DOMContentLoaded', _wirePanelNav);
  // Wiring happens once; which arrows can be offered is asked again on
  // every pass, because a panel appears when a turn paints one and no
  // event says so. Refreshing only at wiring time left the arrows in the
  // state the page had before it had any panels -- present, and never
  // shown.
  setInterval(function () { _wirePanelNav(); _refreshArrowState(); }, 700);

  window.ClientRouterPanelNav = { step: _step, refresh: _refreshArrowState };

  window._gateBody = gateBody;

  window.ClientRouter = {
    render: render,
    merge: merge,
    getStreamedPayloads: getStreamedPayloads,
    getFullPayload: getFullPayload,
    subscribe: subscribe,
    bootstrap: bootstrap,
    getSessionToken: getSessionToken,
    getSessionGuid: getSessionGuid,
    gateBody: gateBody,
  };
})();
