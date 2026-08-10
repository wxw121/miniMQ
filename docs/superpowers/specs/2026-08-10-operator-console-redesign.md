# MiniMQ Operator Console Redesign

## Summary

The current operator console exposes the required workflows but presents every form and action on one dense page with a visual style that does not suit an operational product. Redesign it as an English-language, RabbitMQ-inspired management console with clearer navigation, structured results, and a restrained Swiss visual system.

## Chosen approach

Keep the existing server-rendered static asset model and plain HTML, CSS, and JavaScript. Build a client-side, single-page console shell without adding a frontend framework or changing backend API contracts. This provides the requested product-level redesign while keeping deployment, security boundaries, and local development unchanged.

## Information architecture

The fixed sidebar contains six views:

1. **Overview** — broker target, ops status, runtime metrics, auth status, and failover audit.
2. **Messages** — the create/produce/consume flow and delayed, retry, and dead-letter publishing.
3. **Topics** — list, create, describe, increase partitions, delete, and reliability links.
4. **Consumer groups** — group status, members, and partition information.
5. **Brokers** — broker selection and mark-offline action.
6. **Tools** — shared connection settings, result limits, and access to raw response data.

Sidebar navigation switches views without a page reload and preserves form values and the latest result for each view. The header always identifies MiniMQ, the configured bootstrap server, and connection/configuration state.

## Visual direction

Use a Swiss anchor rather than the current brutalist treatment:

- International Orange `#FF4F00` is the single accent.
- `#F7F7F8` canvas, pure-white content surfaces, near-black text, and neutral 1 px rules.
- Helvetica Neue, Helvetica, or Arial for all display and body text.
- Square major containers, minimal corner radius on small controls, no gradients, grain, or decorative shadows.
- Tabular numerals for metrics, offsets, partitions, and limits.

The differentiator is an orange status rail that begins at the top edge, marks the active sidebar destination, and reappears in selected states, key metrics, and primary actions.

## Components and data flow

Each view uses the same hierarchy: page heading, input/action surface, and contextual result surface. Existing DOM IDs and test IDs required by tests and scripts remain stable where possible.

Requests continue to use the existing payload mapping and endpoints. Results render according to their shape:

- Scalar objects become labeled key-value metrics.
- Arrays of objects become compact, horizontally scrollable tables.
- Nested objects and mixed structures become grouped key-value sections.
- Flow results retain status, mode, offset, and the formatted response.
- Every result includes a collapsible Raw JSON view.

Unknown response shapes fall back to a safe key-value representation and Raw JSON. No values are invented to make the interface look populated.

## Interaction and error handling

- Only the action being submitted is disabled while its request is running.
- Loading, success, warning, error, and empty states include visible text rather than relying on color.
- Field validation is shown next to the relevant control when possible.
- Network, HTTP, and malformed-response errors appear in the current result surface without destroying the last successful result.
- Topic deletion and broker-offline actions require an explicit native confirmation.
- On narrow screens the sidebar becomes a horizontally scrollable navigation row, forms become one column, and tables scroll horizontally.
- Focus outlines remain visible and reduced-motion preferences are respected.

## Testing and acceptance

- Update the existing web asset contract test to require the new shell, navigation, view labels, structured-result hooks, Raw JSON hooks, and Swiss design tokens.
- Preserve coverage for all current operator endpoints and payload mappings.
- Verify the focused `mq-demo` test suite, then run the full Maven test suite.
- Launch the development console and exercise navigation, main flow, structured results, Raw JSON, responsive layout, and dangerous-action confirmation in a real browser.

## Open risks

- Backend responses vary by endpoint; the generic renderer must degrade safely when an unfamiliar nested shape is returned.
- The redesign does not add routing URLs or browser history per view; navigation state exists only for the current page session.
- No dark theme is included because the approved direction is the light RabbitMQ-inspired Swiss console.

## Next skill

`$superpower-writing-plans`
