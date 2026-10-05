"""Check local HTML links and anchors in the built website, including docs."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.ids = set()
        self.links = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.add(attrs['id'])
        if tag == 'a' and 'href' in attrs:
            self.links.append(attrs['href'])


def main():
    root = Path(__file__).resolve().parent / '.site'
    pages = {p: Page(p.read_text()) for p in root.rglob('*.html')}
    assert pages, 'Build the website first'
    errors = []
    for path, page in pages.items():
        if path.name == '404.html':
            continue  # Its relative links depend on the requested URL.
        for link in page.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            target = (path.parent / unquote(url.path)).resolve() if url.path else path
            if target.is_dir():
                target /= 'index.html'
            if not target.exists():
                errors.append(f'{path.relative_to(root)}: missing target {link}')
            elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].ids:
                errors.append(f'{path.relative_to(root)}: missing anchor {link}')
    if errors:
        raise SystemExit('\n'.join(errors))
    print(f'Checked local links and anchors in {len(pages)} HTML pages')


if __name__ == '__main__':
    main()
