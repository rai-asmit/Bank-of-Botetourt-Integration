'use strict';

// HubSpot SDK errors stringify the entire HTTP response (headers, cookies,
// cf-ray IDs, the lot) into err.message — which is unreadable in logs.
// This helper pulls out the parts that actually matter.

function parseBody(err) {
  const direct = err && err.response && err.response.body;
  if (direct && typeof direct === 'object') return direct;
  if (typeof direct === 'string') {
    try { return JSON.parse(direct); } catch (_) { /* fall through */ }
  }
  // The SDK formats: "HTTP-Code: N\nMessage: ...\nBody: {json}\nHeaders: {json}"
  // We slice between the literal "Body:" and "\nHeaders:" markers; this is
  // immune to nested braces inside the body.
  const msg = err && err.message;
  if (!msg) return null;
  const bodyStart = msg.indexOf('Body:');
  if (bodyStart === -1) return null;
  const after = msg.indexOf('\nHeaders:', bodyStart);
  const bodyText = msg.slice(bodyStart + 'Body:'.length, after === -1 ? undefined : after).trim();
  if (!bodyText.startsWith('{')) return null;
  try { return JSON.parse(bodyText); } catch (_) { return null; }
}

// Pull HTTP status out of the SDK's "HTTP-Code: 400" prefix when neither
// err.code nor err.response.status is set.
function pickStatusFromMessage(msg) {
  if (!msg) return null;
  const m = msg.match(/HTTP-Code:\s*(\d+)/i);
  return m ? Number(m[1]) : null;
}

function pickValueFromMessage(msg) {
  if (!msg) return null;
  // "Email address brittfarminva09@gmai.lcom is invalid"  → grab the email
  const emailMatch = msg.match(/Email address ([^\s]+) is invalid/i);
  if (emailMatch) return emailMatch[1];
  // generic "X is invalid"
  const generic = msg.match(/^(.+?) is invalid/i);
  if (generic) return generic[1];
  return null;
}

/**
 * Reduce a HubSpot SDK error to a short, human-readable summary.
 * Returns { status, code, category, propertyName, value, summary }.
 */
function summarize(err) {
  const status   = (err && err.code)
                || (err && err.response && err.response.status)
                || pickStatusFromMessage(err && err.message)
                || null;
  const body     = parseBody(err);
  const firstErr = body && Array.isArray(body.errors) && body.errors[0];

  const code         = firstErr && firstErr.code           || body && body.category || null;
  const category     = body && body.category               || null;
  const propertyName = firstErr && firstErr.context && firstErr.context.propertyName && firstErr.context.propertyName[0] || null;
  const detail       = (firstErr && firstErr.message) || (body && body.message) || (err && err.message) || 'unknown error';
  const value        = pickValueFromMessage(detail);

  let summary;
  if (code === 'INVALID_EMAIL' && value) {
    summary = ` invalid email "${value}"`;
  } else if (code && propertyName && value) {
    summary = ` ${code} on "${propertyName}": "${value}"`;
  } else if (code) {
    summary = ` ${code}: ${detail}`;
  } else if (status === 429) {
    summary = ` rate-limited (HTTP 429)`;
  } else if (status && status >= 500) {
    summary = ` HubSpot ${status} server error`;
  } else if (status) {
    summary = `HTTP ${status}: ${detail}`;
  } else {
    summary = `${detail}`;
  }

  return { status, code, category, propertyName, value, summary };
}

module.exports = { summarize };
