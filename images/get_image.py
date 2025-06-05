from pathlib import Path
import re


def get_image_name(name: str) -> str:
    """Returns the base image of /images/{name}/Dockerfile.

    Arguments:
        name (str): The name of the subdirectory in /dags/images that contains
                    the Dockerfile. May contain slashes for nested subdirectories.
    """
    root = Path(__file__).parent

    dockerfile = root / f"{name}/Dockerfile"
    if not dockerfile.exists():
        raise ValueError(f"Could not find {dockerfile}")

    with dockerfile.open("r", encoding="utf-8") as f:
        matches = re.findall(r"^FROM\s+(.*)", f.read(), flags=re.MULTILINE)
        if len(matches) == 1:
            return matches[0].strip()
        else:
            raise RuntimeError(f"Could not find a unique image reference in {dockerfile}")
