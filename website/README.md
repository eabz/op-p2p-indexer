# op-p2p-indexer landing page

Static branding and product landing page. The source of truth lives in this repository under `website/dist/`; there is no build step or package installation.

## Preview

From the repository root:

```sh
python3 -m http.server 4173 --directory website/dist
```

Open http://localhost:4173. The hero is a labeled simulation, not a connection to a running indexer.

## Edit

- `dist/index.html`: product copy, architecture, links and setup commands.
- `dist/style.css`: brand colors, typography, responsive layout and motion preferences.
- `dist/app.js`: chain and pipeline selection, simulation controls, protocol examples and clipboard action.

The quick start currently builds from source. When an installer is released and its usage documented, replace the commands in `#install-code` and update the requirements beside them. The copy button reads the displayed commands automatically. Keep the instructions consistent with the root README; do not advertise an unreleased download URL.

## Publish

`.openai/hosting.json` identifies the existing private Site and its static output directory. Use the Sites publishing workflow with this identity; do not register a replacement Site. Prepare a separate temporary checkout containing `website/` when publishing, so the Sites workflow operates independently of this repository's Git metadata. Keep changes here as the source of truth.

Before publishing, run `node --check website/dist/app.js` from the repository root and check chain selection, pipeline stages, pause/play, protocol tabs, copy commands and the mobile layout.
