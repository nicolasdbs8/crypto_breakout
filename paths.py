from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = DATA_DIR / "outputs"


def ensure_output_dir() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def output_path(filename: str) -> Path:
    ensure_output_dir()
    return OUTPUT_DIR / filename


def output_path_str(filename: str) -> str:
    return str(output_path(filename))


def resolve_input_path(filename: str) -> Path:
    out = OUTPUT_DIR / filename
    if out.exists():
        return out
    root = ROOT / filename
    if root.exists():
        return root
    return out


def resolve_input_path_str(filename: str) -> str:
    return str(resolve_input_path(filename))
