"""Optional Chromium dashboard check. Screenshots go to private runtime output."""
import argparse
from pathlib import Path


def check(url, token_file, output, channel):
    from playwright.sync_api import sync_playwright
    from urllib.parse import urlsplit
    if urlsplit(url).hostname not in {'127.0.0.1','localhost','::1'}:
        raise ValueError('Local dashboard URL required')
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as driver:
        browser = driver.chromium.launch(channel=channel)
        for name, width, height in [('desktop', 1440, 900), ('mobile', 390, 844)]:
            page = browser.new_page(viewport={'width': width, 'height': height})
            errors = []
            page.on('pageerror', lambda error: errors.append(type(error).__name__))
            page.goto(url)
            page.locator('#access-token').fill(token_file.read_text().strip())
            page.locator('#sign-in-form button').click()
            page.locator('#kpis > *').first.wait_for()
            for tab in ('overview','customers','analytics','reconciliation'):
                page.locator(f'[data-page="{tab}"]').click()
                page.locator(f'#{tab}').wait_for()
                if page.evaluate('document.documentElement.scrollWidth > innerWidth'):
                    raise ValueError(f'Horizontal overflow on {name} {tab}')
                page.screenshot(path=output / f'{name}-{tab}.png', full_page=True)
            page.locator('#sign-out').click()
            page.locator('#access-token').wait_for()
            if errors:
                raise ValueError('Browser JavaScript error')
            page.close()
        browser.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8090')
    parser.add_argument('--token-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path('runtime/browser-acceptance'))
    parser.add_argument('--channel', default='chrome')
    args = parser.parse_args()
    check(args.url, args.token_file, args.output, args.channel)
