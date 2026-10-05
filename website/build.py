"""Build the landing page and docs from repository Markdown. Run from any directory."""
from pathlib import Path
import io
import json
import re
import shutil
import subprocess
import zipfile
from mkdocs.config import load_config
from mkdocs.commands.build import build

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
STAGED = HERE / '.docs'
OUTPUT = HERE / '.site'
BASE = 'https://eabz.github.io/op-p2p-indexer/'
REPO = 'https://github.com/eabz/op-p2p-indexer/blob/'


def release_file(version, path):
    return subprocess.run(['git', 'show', f'{version}:{path}'], cwd=ROOT, check=True, capture_output=True).stdout


def main():
    version = json.loads((HERE / 'release.json').read_text())['version']
    if not re.fullmatch(r'v\d+\.\d+\.\d+', version):
        raise ValueError('release.json must select an existing release tag')
    if f'--version {version}' not in (HERE / 'dist/index.html').read_text():
        raise ValueError('landing install command must pin the selected release')
    development = HERE / '.docs-development'
    for directory in (STAGED, OUTPUT, development):
        if directory.exists():
            shutil.rmtree(directory)
    # Keep the installer, protocol and technical reference on the same release.
    archive = subprocess.run(['git', 'archive', '--format=zip', version, 'docs'], cwd=ROOT, check=True, capture_output=True).stdout
    STAGED.mkdir()
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        bundle.extractall(STAGED)
    for path in (STAGED / 'docs').iterdir():
        shutil.move(str(path), STAGED / path.name)
    (STAGED / 'docs').rmdir()
    shutil.copytree(ROOT / 'docs/guides', STAGED / 'guides')
    shutil.copytree(ROOT / 'docs/examples', STAGED / 'examples')
    shutil.copy2(ROOT / 'docs/index.md', STAGED / 'index.md')
    (STAGED / 'examples/stream.proto').write_bytes(release_file(version, 'crates/stream/proto/opindexer/v1/stream.proto'))
    shutil.copytree(ROOT / 'docs', development)
    shutil.rmtree(development / 'guides')
    shutil.rmtree(development / 'examples')
    development_notice = '> **Development reference:** setup and configuration changes here may require source-built binaries. The public installer and release guides target ' + version + '; use those for published binaries.\n\n'
    (development / 'index.md').write_text(development_notice + (ROOT / 'README.md').read_text().replace('](docs/', ']('))
    for source, ref in ((STAGED, version), (development, 'main')):
        for path in source.rglob('*.md'):
            content = path.read_text()
            for filename in ('config.toml.example', 'LICENSE'):
                content = content.replace(f'](../{filename})', f']({REPO}{ref}/{filename})').replace(f']({filename})', f']({REPO}{ref}/{filename})')
            path.write_text(content)
        for asset in ('brand.css', 'logo.svg'):
            shutil.copy2(HERE / 'docs-theme' / asset, source / asset)
    shutil.copytree(HERE / 'dist', OUTPUT)
    (OUTPUT / 'install.sh').write_bytes(release_file(version, 'scripts/install.sh'))
    shutil.copy2(HERE / 'release.json', OUTPUT / 'release.json')
    config = load_config(str(HERE / 'mkdocs.yml'), strict=True)
    config['extra']['version_label'] = f'{version} · Release guide'
    build(config)
    dev_config = load_config(str(HERE / 'mkdocs.yml'), strict=True)
    dev_config['docs_dir'] = str(development)
    dev_config['site_dir'] = str(OUTPUT / 'docs/development')
    dev_config['site_url'] = BASE + 'docs/development/'
    dev_config['nav'] = [{'Development overview': 'index.md'}, {'Engineering reference': [
        {p.stem.replace('-', ' ').title(): p.name} for p in sorted(development.glob('*.md')) if p.name != 'index.md'
    ]}]
    dev_config['extra']['version_label'] = 'Development · main · May differ from releases'
    dev_config['extra']['homepage'] = '../../'
    dev_config['extra']['release_docs'] = '../'
    dev_config['extra']['development_docs'] = './'
    build(dev_config)
    export_markdown(STAGED, OUTPUT / 'docs', BASE + 'docs/', version)
    export_markdown(development, OUTPUT / 'docs/development', BASE + 'docs/development/', 'development main')
    for name in ('llms.txt', 'llms-full.txt'):
        shutil.copy2(OUTPUT / 'docs' / name, OUTPUT / name)
    (OUTPUT / '.nojekyll').touch()
    print(f'Built website and {version} / development documentation in {OUTPUT}')


def export_markdown(source, destination, base, version):
    markdown = destination / 'markdown'
    index = ['# op-p2p-indexer', '', f'> Documentation baseline: {version}. Self-hosted OP Stack data.', '', '## Documentation', '']
    full = [f'# op-p2p-indexer documentation ({version})', '']
    for path in sorted(source.rglob('*.md')):
        relative = path.relative_to(source)
        content = path.read_text()
        target = markdown / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        title = re.search(r'^# (.+)$', content, re.MULTILINE)
        title = title.group(1) if title else relative.stem
        url = f'{base}markdown/{relative.as_posix()}'
        index.append(f'- [{title}]({url})')
        full.extend([f'<!-- Source: {url} -->', content, ''])
    # Preserve downloadable benchmark evidence linked from the Markdown mirrors.
    for name in ('benchmarks', 'examples'):
        if (source / name).exists():
            shutil.copytree(source / name, markdown / name)
    (destination / 'llms.txt').write_text('\n'.join(index) + '\n')
    (destination / 'llms-full.txt').write_text('\n'.join(full))


if __name__ == '__main__':
    main()
