/*
 * Monaco-based JSON editor for dataset configs (submit / edit / view pages).
 *
 * Progressive enhancement: the server renders a plain <textarea id="submit_box">.
 * If Monaco loads, the textarea is hidden and the editor's text is copied back
 * into it when the form is submitted. If anything fails (JS disabled, CDN
 * unreachable, blocked by CSP), the textarea stays visible and works as before.
 *
 * Schema validation is advisory only: schema problems show as warnings, JSON
 * syntax problems as errors, and submitting is never blocked. The server
 * remains the authoritative validator.
 *
 * Configuration comes from data-* attributes on <div id="config_editor">:
 *   data-monaco-base             CDN URL of Monaco's "min/vs" directory
 *   data-read-only               "1" for the view-only page
 *   data-schema-versions         JSON list of available schema versions, e.g. [3.1, 3.2]
 *   data-schema-default-version  version to use when the config has no "version"
 *                                (legacy 3.1 for old/existing configs, latest for new submissions)
 *   data-schema-force-version    new submissions only: the server always validates and stores
 *                                this (latest) version, whatever "version" says
 *   data-error-line / -col       optional 1-based position of a server-side JSON error
 *   data-error-message           optional message for that position
 */
(function () {
  'use strict';

  var textarea = document.getElementById('submit_box');
  var container = document.getElementById('config_editor');
  if (!textarea || !container || typeof window.require !== 'function') {
    return;  // keep the plain textarea
  }

  // Monaco injects <style> elements at runtime without a nonce, which our CSP
  // (style-src with a nonce) would block. Give every <style> created from now
  // on the page's nonce, which this <script> tag carries. This keeps the strict
  // style-src instead of falling back to 'unsafe-inline'.
  var nonce = document.currentScript && document.currentScript.nonce;
  if (nonce) {
    var origCreateElement = document.createElement;
    document.createElement = function (tagName, options) {
      var el = origCreateElement.call(document, tagName, options);
      if (String(tagName).toLowerCase() === 'style') {
        el.nonce = nonce;
      }
      return el;
    };
  }

  var opts = container.dataset;
  var readOnly = opts.readOnly === '1';
  var schemaVersions = [];
  try {
    schemaVersions = JSON.parse(opts.schemaVersions || '[]').map(Number);
  } catch (e) {
    console.warn('config_editor: bad schema version list', e);
  }
  var defaultVersion = Number(opts.schemaDefaultVersion);
  var forceVersion = opts.schemaForceVersion ? Number(opts.schemaForceVersion) : null;

  /** Round to one decimal, like ConfigSchema.schema() does on the server. */
  function roundVersion(v) {
    return Math.round(Number(v) * 10) / 10;
  }

  /**
   * Read the "version" value from the config text, or undefined.
   * Uses JSON.parse when possible, otherwise a regex so that a config with a
   * syntax error elsewhere still gets the right schema.
   */
  function readVersion(text) {
    var ver;
    try {
      var obj = JSON.parse(text);
      if (obj && typeof obj === 'object' && 'version' in obj) {
        ver = obj.version;
      }
    } catch (e) {
      var m = /"version"\s*:\s*"?(\d+(?:\.\d+)?)"?/.exec(text);
      if (m) {
        ver = m[1];
      }
    }
    if (ver === undefined || ver === null || ver === '' || isNaN(Number(ver))) {
      return undefined;
    }
    return roundVersion(ver);
  }

  /** Pick the schema version to validate the current text against. */
  function detectVersion(text) {
    if (forceVersion !== null) {
      return forceVersion;
    }
    var ver = readVersion(text);
    if (ver === undefined) {
      return defaultVersion;  // no version: see data-schema-default-version
    }
    if (schemaVersions.indexOf(ver) < 0) {
      console.info('config_editor: unknown schema version ' + ver + ', using ' + defaultVersion);
      return defaultVersion;
    }
    return ver;
  }

  function schemaUrl(ver) {
    return new URL('/schemas/v3/dataset_v' + ver.toFixed(1) + '.schema.json', window.location.href).href;
  }

  var schemaCache = {};
  function fetchSchema(ver) {
    if (!schemaCache[ver]) {
      schemaCache[ver] = fetch(schemaUrl(ver), {credentials: 'same-origin'}).then(function (r) {
        if (!r.ok) {
          throw new Error('HTTP ' + r.status);
        }
        return r.json();
      });
    }
    return schemaCache[ver];
  }

  window.require.config({paths: {vs: opts.monacoBase}});
  window.require(['vs/editor/editor.main'], function () {
    var monaco = window.monaco;
    var modelUri = monaco.Uri.parse('inmemory://iceprod/dataset-config.json');
    var model = monaco.editor.createModel(textarea.value, 'json', modelUri);

    var editor = monaco.editor.create(container, {
      model: model,
      readOnly: readOnly,
      automaticLayout: true,
      lineNumbers: 'on',
      minimap: {enabled: false},
      scrollBeyondLastLine: false,
      tabSize: 2,
      folding: true,
      renderValidationDecorations: 'on',  // show markers in read-only mode too
    });

    // swap textarea for editor
    textarea.hidden = true;
    container.hidden = false;
    if (textarea.form) {
      textarea.form.addEventListener('submit', function () {
        textarea.value = editor.getValue();
      });
    }

    // --- schema selection -------------------------------------------------
    /** New submissions: warn on the "version" line if it differs from the forced version. */
    function checkForcedVersion(text) {
      var markers = [];
      var ver = readVersion(text);
      if (forceVersion !== null && ver !== undefined && ver !== forceVersion) {
        // nested objects may also have a "version" key; the top-level one is
        // the least indented (best effort, good enough for a warning)
        var match = null;
        model.findMatches('"version"\\s*:\\s*"?[0-9.]+"?', false, true, false, null, false).forEach(function (m) {
          if (!match || m.range.startColumn < match.range.startColumn) {
            match = m;
          }
        });
        if (match) {
          markers.push({
            severity: monaco.MarkerSeverity.Warning,
            message: 'New submissions are always validated and stored as version ' +
                     forceVersion + '; this value will be replaced.',
            startLineNumber: match.range.startLineNumber,
            startColumn: match.range.startColumn,
            endLineNumber: match.range.endLineNumber,
            endColumn: match.range.endColumn,
          });
        }
      }
      monaco.editor.setModelMarkers(model, 'version', markers);
    }

    var currentVersion = null;
    function updateSchema() {
      var text = model.getValue();
      checkForcedVersion(text);
      var ver = detectVersion(text);
      if (ver === currentVersion || isNaN(ver)) {
        return;
      }
      currentVersion = ver;
      fetchSchema(ver).then(function (schema) {
        if (ver !== currentVersion) {
          return;  // changed again while loading
        }
        monaco.languages.json.jsonDefaults.setDiagnosticsOptions({
          validate: true,
          allowComments: false,
          trailingCommas: 'error',
          enableSchemaRequest: false,
          schemaValidation: 'warning',  // advisory
          schemas: [{
            uri: schemaUrl(ver),
            fileMatch: [modelUri.toString()],
            schema: schema,
          }],
        });
      }).catch(function (e) {
        console.warn('config_editor: could not load schema v' + ver, e);
      });
    }
    updateSchema();
    var debounce = null;
    model.onDidChangeContent(function () {
      clearTimeout(debounce);
      debounce = setTimeout(updateSchema, 500);
    });

    // --- server-side error position --------------------------------------
    var errLine = parseInt(opts.errorLine || '', 10);
    if (errLine > 0) {
      var errCol = parseInt(opts.errorCol || '', 10) || 1;
      monaco.editor.setModelMarkers(model, 'server', [{
        severity: monaco.MarkerSeverity.Error,
        message: opts.errorMessage || 'Server error',
        startLineNumber: errLine,
        startColumn: errCol,
        endLineNumber: errLine,
        endColumn: model.getLineMaxColumn(Math.min(errLine, model.getLineCount())),
      }]);
      editor.revealLineInCenter(errLine);
      editor.setPosition({lineNumber: errLine, column: errCol});
      // the server's marker is stale once the user edits
      var sub = model.onDidChangeContent(function () {
        monaco.editor.setModelMarkers(model, 'server', []);
        sub.dispose();
      });
    }
    if (!readOnly) {
      editor.focus();
    }
  }, function (err) {
    console.warn('config_editor: Monaco failed to load, using plain textarea', err);
  });
})();
