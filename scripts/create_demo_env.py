"""Generate a local ignored .env without printing secrets or replacing existing config."""
from pathlib import Path
import secrets


def main():
    target=Path('.env')
    if target.exists():
        raise SystemExit('.env already exists; preserved')
    text=Path('.env.example').read_text()
    password=secrets.token_urlsafe(32)
    user='demo_'+secrets.token_hex(6)
    text=text.replace('replace-with-local-password',password).replace('replace-with-local-user',user)
    with target.open('x',encoding='utf-8') as handle:
        handle.write(text)
    print('Created ignored local .env')


if __name__ == '__main__':
    main()
