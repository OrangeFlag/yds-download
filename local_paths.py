"""Map source paths to bounded local names without changing remote identities."""
import hashlib
from pathlib import PurePosixPath


def local_path(root, source_path):
    path = PurePosixPath(source_path)
    if (not path.is_absolute() or len(path.parts) < 2 or '..' in path.parts
            or any(ord(c) < 32 for c in source_path)):
        raise ValueError('Unsafe source path')
    names = []
    for name in path.parts[1:]:
        raw = name.encode('utf-8')
        # Reserve bytes for aria2 controls, Range journals and partial-copy suffixes.
        if len(raw) > 230:
            extension = PurePosixPath(name).suffix
            if len(extension.encode('utf-8')) > 32:
                extension = ''
            suffix = '~' + hashlib.sha256(raw).hexdigest()[:16] + extension
            name = raw[:230 - len(suffix.encode('utf-8'))].decode('utf-8', errors='ignore') + suffix
        names.append(name)
    destination = root.joinpath(*names)
    if not destination.resolve().is_relative_to(root.resolve()):
        raise ValueError('Unsafe local path')
    return destination
