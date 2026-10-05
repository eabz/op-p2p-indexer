# Website and documentation

The landing page lives in `dist/`. The documentation uses Material for MkDocs and builds
from Markdown. Public guides live in `docs/index.md` and `docs/guides/`; the technical reference
comes from the Git tag selected in `release.json`. `/docs/development/` separately renders
the working tree's engineering docs and README. Edit sources, not generated files.

## Build and preview

From the repository root:

```sh
git fetch --tags
python3 -m venv website/.venv
website/.venv/bin/pip install -r website/requirements.txt
website/.venv/bin/python website/build.py
python3 -m http.server 4173 --directory website/.site
```

Open http://localhost:4173 for the website or http://localhost:4173/docs/ for the docs.
Rebuild after editing. Generated `.docs/` and `.site/` directories are ignored by Git.

## Editing

- `dist/index.html`: landing page content; keep install and documentation as the main actions.
- `dist/style.css`: the charcoal/coral brand, responsive layout and reduced-motion support.
- `dist/app.js`: data-flow controls, API examples and clipboard feedback.
- `mkdocs.yml`: documentation navigation, search, code copying and Mermaid diagrams.
- `docs-theme/`: matching colors and links to each page's Markdown source.
- `build.py`: stage the existing Markdown, compile the site and export agent-readable docs.

The installer is `scripts/install.sh` from this checkout, published as `/install.sh`. It takes
no options and installs the latest release itself, so the landing command pins no version.
The data-flow diagram describes the architecture; it does not show live measurements.

## Documentation for readers and agents

- `/docs/`: searchable HTML, grouped navigation, section permalinks and code-copy buttons.
- `/docs/markdown/<name>.md`: plain Markdown for each page, linked from its HTML header.
- `/llms.txt` and `/docs/llms.txt`: an index of the Markdown URLs.
- `/llms-full.txt` and `/docs/llms-full.txt`: the complete documentation text.

These are all generated from the same source files. Benchmark evidence is preserved as
static downloads. `llms.txt` aids discovery; agents can also read the rendered HTML or fetch
individual Markdown pages without JavaScript.

Both the release and development docs have their own Markdown exports and agent indexes.
The root agent index selects the release documentation. Do not mix development guidance
with installed releases. Downloadable examples and the released protobuf schema are under
`/docs/examples/`.

## Publish

The existing `.github/workflows/pages.yml` builds and publishes both the landing page and
docs to GitHub Pages when `website/`, `docs/`, `README.md`, or `scripts/install.sh` changes on
`main`. It can also be run manually. Repository Settings → Pages must use GitHub Actions.
There is no separate documentation hosting account or service.

Before publishing:

```sh
node --check website/dist/app.js
website/.venv/bin/python website/build.py
website/.venv/bin/python website/check_links.py
website/.venv/bin/pip install -r website/requirements-check.txt
website/.venv/bin/python website/check_examples.py
```

Check desktop and mobile layouts, install copy feedback, data-flow controls, documentation
navigation, search, Mermaid rendering and Markdown links in the browser. The MkDocs build
runs in strict mode so documentation build warnings fail CI.

The PR check compiles the released protocol and runs the downloadable query against a local
Flight fixture, including its gap and truncation diagnostics.
