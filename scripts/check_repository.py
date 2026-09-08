"""Conservative tracked-file hygiene checks, not a credential-discovery tool."""
from pathlib import Path
import re
import subprocess


def main():
    tracked=subprocess.check_output(['git','ls-files','-z']).decode().split('\0')
    errors=[]
    forbidden={'full_name','email','phone','date_of_birth'}
    for name in filter(None,tracked):
        path=Path(name)
        if path.name == '.env' or path.suffix in {'.tfstate','.pem','.key','.plan'} or 'runtime/' in name:
            errors.append(f'Forbidden tracked artifact: {name}')
        content=path.read_text(encoding='utf-8',errors='replace')
        if re.search(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',content):
            errors.append(f'Private key in {name}')
        if re.search(r'(?:AKIA|ASIA)[A-Z0-9]{16}',content):
            errors.append(f'Access-key-shaped value in {name}')
        if re.search(r'[A-Z]:[\\/]Users[\\/]',content):
            errors.append(f'Absolute developer path in {name}')
        if name.startswith('dbt/models/') and path.suffix=='.sql':
            sql=re.sub(r'--.*','',content)
            if any(re.search(r'\b'+field+r'\b',sql) for field in forbidden):
                errors.append(f'Raw PII selected by Gold model: {name}')
    if errors:
        raise SystemExit('\n'.join(errors))
    print('PASS: tracked artifact, credential-pattern, path and Gold PII hygiene')


if __name__ == '__main__':
    main()
