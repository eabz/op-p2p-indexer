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

The quick start installs the latest published Linux release through `scripts/install.sh`.
It uses `--binaries-only` so it also works with older releases that predate guided TOML setup;
the adjacent guide covers chain configuration and systemd services. The one-liner includes
Ubuntu's curl/CA prerequisites. The copy button reads the displayed command automatically.
Keep the instructions consistent with the root README; no Rust build is required.

## Publish

GitHub Pages serves `website/dist/` at https://eabz.github.io/op-p2p-indexer/.

The workflow in `.github/workflows/pages.yml` deploys automatically when website files change on `main`. It can also be run manually from GitHub Actions. Repository Settings → Pages must use **GitHub Actions** as the publishing source. Only the static files are uploaded; no build step, hosting credentials or external hosting service is required.

Before publishing, run `node --check website/dist/app.js` from the repository root and check chain selection, pipeline stages, pause/play, protocol tabs, copy commands and the mobile layout.
