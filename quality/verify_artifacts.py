"""Verify the license payload and Java baseline of every distributable library archive."""
from pathlib import Path
import struct
from zipfile import ZipFile


def verify_archive(path, expected):
    """Require exact notices and Java 8 class files in one generated JAR."""
    with ZipFile(path) as archive:
        for name, content in expected.items():
            if archive.read(name) != content:
                raise ValueError(f'Incorrect notice {name} in {path}')
        for name in archive.namelist():
            if name.endswith('.class'):
                magic, _, major = struct.unpack('>IHH', archive.read(name)[:8])
                if magic != 0xCAFEBABE or major != 52:
                    raise ValueError(f'Class outside the Java 8 baseline: {path}:{name}')


def main():
    """Check binary, source, and Javadoc JARs for each production module."""
    root = Path(__file__).resolve().parent.parent
    expected = {
        'META-INF/LICENSE': (root / 'LICENSE').read_bytes(),
        'META-INF/NOTICE': (root / 'NOTICE').read_bytes(),
        'META-INF/licenses/raknetify2-quality-LICENSE': (root / 'quality/LICENSE').read_bytes(),
    }
    count = 0
    for module in ('common', 'client', 'server'):
        archives = sorted((root / module / 'build/libs').glob('*.jar'))
        if len(archives) != 3:
            raise ValueError(f'Expected binary, source, and Javadoc archives for {module}')
        for path in archives:
            verify_archive(path, expected)
            count += 1
    print(f'Verified notices and Java baseline in {count} library archives')


if __name__ == '__main__':
    main()
