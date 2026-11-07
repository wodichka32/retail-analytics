import argparse
import shutil
from pathlib import Path

import kagglehub

from src.utils.config import KAGGLE_DATASETS, RAW_DIR


def download(source: str, force: bool = False) -> None:
    target = RAW_DIR / source
    target.mkdir(parents=True, exist_ok=True)

    cache_dir = kagglehub.dataset_download(KAGGLE_DATASETS[source])
    files = sorted(p for p in Path(cache_dir).iterdir() if p.is_file())

    for src in files:
        dst = target / src.name
        if dst.exists() and not force:
            print(f"skip  {dst.name} (already exists)")
            continue
        shutil.copy2(src, dst)
        print(f"copy  {dst.name}  {dst.stat().st_size / 1e6:.1f} MB")

    print(f"{source}: {len(files)} files in {target}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download raw datasets from Kaggle into data/raw/<source>/")
    parser.add_argument("source", choices=sorted(KAGGLE_DATASETS))
    parser.add_argument("--force", action="store_true", help="overwrite existing files")
    args = parser.parse_args()
    download(args.source, args.force)


if __name__ == "__main__":
    main()
