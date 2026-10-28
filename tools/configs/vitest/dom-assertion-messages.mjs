import assert from 'node:assert';
import { syncBuiltinESMExports } from 'node:module';
import { inspect } from 'node:util';

/**
 * Vitest setup file for jsdom suites that use Node's `assert`.
 *
 * When a comparison fails, Node describes both values with `util.inspect` at
 * unlimited depth and with custom inspectors turned off. `strictEqual` and
 * `deepStrictEqual` do this even when the test passes its own message. React
 * stores its fiber on every DOM node it renders, so describing one element
 * walks the whole component tree. A single failing
 * `assert.equal(element, null)` can run until the Vitest worker dies with
 * "Worker exited unexpectedly" instead of reporting the failure.
 *
 * This wraps the comparison methods. When a DOM node is compared, the wrapper
 * throws the `AssertionError` itself, with the test's message (if any) and the
 * node's HTML. Vitest still prints its own diff of the two values below it.
 */

const MAX_LENGTH = 300;
const ORIGINAL = Symbol.for('ghost.domAssertionMessages.original');

// Node throws an `Error` passed as the message as-is, without describing the
// values
const FAILED = new Error('DOM assertion failed');

// Keyed by the name of the function, which is what decides the operator:
// `assert.strict.equal` is `strictEqual`
const OPERATORS = {
  equal: '==',
  notEqual: '!=',
  strictEqual: 'strictEqual',
  notStrictEqual: 'notStrictEqual',
  deepEqual: 'deepEqual',
  notDeepEqual: 'notDeepEqual',
  deepStrictEqual: 'deepStrictEqual',
  notDeepStrictEqual: 'notDeepStrictEqual',
  partialDeepStrictEqual: 'partialDeepStrictEqual',
};

function isDomNode(value) {
  return typeof globalThis.Node === 'function' && value instanceof globalThis.Node;
}

function isDomCollection(value) {
  return (
    (Array.isArray(value) ||
      (typeof globalThis.NodeList === 'function' && value instanceof globalThis.NodeList) ||
      (typeof globalThis.HTMLCollection === 'function' &&
        value instanceof globalThis.HTMLCollection)) &&
    Array.prototype.some.call(value, isDomNode)
  );
}

function truncate(text) {
  return text.length > MAX_LENGTH ? `${text.slice(0, MAX_LENGTH)}…` : text;
}

function describe(value) {
  if (isDomNode(value)) {
    return truncate(value.outerHTML ?? `${value.nodeName} ${JSON.stringify(value.textContent)}`);
  }
  if (isDomCollection(value)) {
    return truncate(`[${Array.prototype.map.call(value, describe).join(', ')}]`);
  }
  return truncate(inspect(value));
}

function wrap(name, original) {
  const operator = OPERATORS[original.name] ?? original.name;

  const wrapped = function (actual, expected, message) {
    if (
      message instanceof Error ||
      ![actual, expected].some((value) => isDomNode(value) || isDomCollection(value))
    ) {
      return original.apply(this, arguments);
    }
    try {
      return original.call(this, actual, expected, FAILED);
    } catch (error) {
      if (error !== FAILED) {
        throw error;
      }
      const failure = new assert.AssertionError({
        message: `${message ?? `assert.${name} failed`}\n\nactual:   ${describe(actual)}\nexpected: ${describe(expected)}`,
        actual,
        expected,
        stackStartFn: wrapped,
      });
      // Set afterwards: given the operator, the constructor describes the
      // values again
      failure.operator = operator;
      throw failure;
    }
  };
  wrapped[ORIGINAL] = original;
  return wrapped;
}

// `assert.strict` is the object `assert/strict` exports
for (const target of [assert, assert.strict]) {
  for (const name of Object.keys(OPERATORS)) {
    const original = target[name];
    if (typeof original === 'function' && !original[ORIGINAL]) {
      target[name] = wrap(name, original);
    }
  }
}

// Update the named exports seen by `import * as assert from 'assert/strict'`
syncBuiltinESMExports();
