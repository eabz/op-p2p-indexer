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

The displayed command uses the short Pages endpoint:

```sh
curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh | bash
```

The installer takes no options; it asks which programs to install in a menu. The copy
button reads the displayed command automatically. Keep it consistent with the root README;
no Rust build is required.

## Publish

GitHub Pages serves `website/dist/` at https://eabz.github.io/op-p2p-indexer/.

The workflow in `.github/workflows/pages.yml` deploys automatically when website files or `scripts/install.sh` change on `main`. It copies the canonical `scripts/install.sh` into the Pages artifact as `install.sh`; do not maintain a second tracked installer under `website/dist/`. The endpoint therefore follows main, while downloaded binaries always come from checksum-verified releases. It can also be run manually from GitHub Actions. Repository Settings → Pages must use **GitHub Actions** as the publishing source. Only the static files are uploaded; no build step, hosting credentials or external hosting service is required.

Before publishing, run `node --check website/dist/app.js` from the repository root and check chain selection, pipeline stages, pause/play, protocol tabs, copy commands and the mobile layout.
