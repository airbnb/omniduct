import os


def ensure_path_exists(path):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path
