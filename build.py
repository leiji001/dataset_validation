"""PyInstaller 打包脚本。"""

import PyInstaller.__main__


def build() -> None:
    """将 main.py 打包为单文件可执行程序。"""
    PyInstaller.__main__.run([
        "main.py",
        "--onefile",
        "--console",
        "--name", "dataset_validation",
        "--clean",
    ])


if __name__ == "__main__":
    build()
