# -*- mode: python ; coding: utf-8 -*-
# 构建：scripts/build.sh
import re
VERSION = re.search(r"VERSION = \"(.+?)\"", open("guard/__init__.py").read()).group(1)

a = Analysis(
    ["run.py"],
    datas=[("guard/static", "guard/static")],
    hiddenimports=["keyring.backends.macOS"],
    excludes=["tkinter", "pytest", "PyInstaller"],
)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name="PositionGuard", console=False, target_arch="arm64")
coll = COLLECT(exe, a.binaries, a.datas, name="PositionGuard")
app = BUNDLE(
    coll,
    name="PositionGuard.app",
    icon="build/icon.icns",
    bundle_identifier="com.positionguard.app",
    version=VERSION,
    info_plist={
        "CFBundleDisplayName": "仓位哨兵",
        "CFBundleName": "PositionGuard",
        "CFBundleShortVersionString": VERSION,
        "LSUIElement": True,
        "LSMinimumSystemVersion": "11.0",
        "NSHighResolutionCapable": True,
    },
)
