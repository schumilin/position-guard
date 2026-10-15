"""生成应用图标：深色圆角底 + 绿色盾牌 + 对勾。输出 build/icon.icns。"""
import subprocess
from pathlib import Path

from PIL import Image, ImageDraw

out = Path(__file__).resolve().parent.parent / "build"
iconset = out / "icon.iconset"
iconset.mkdir(parents=True, exist_ok=True)

S = 1024
img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
d.rounded_rectangle([64, 64, S - 64, S - 64], radius=200, fill=(20, 26, 38, 255))
shield = [(512, 190), (790, 290), (770, 560), (512, 840), (254, 560), (234, 290)]
d.polygon(shield, fill=(34, 197, 94, 255))
d.line([(380, 520), (480, 620), (660, 420)], fill=(20, 26, 38, 255), width=70, joint="curve")
for size in (16, 32, 64, 128, 256, 512):
    img.resize((size, size), Image.LANCZOS).save(iconset / f"icon_{size}x{size}.png")
    img.resize((size * 2, size * 2), Image.LANCZOS).save(iconset / f"icon_{size}x{size}@2x.png")
subprocess.run(["iconutil", "-c", "icns", str(iconset), "-o", str(out / "icon.icns")], check=True)
print("icon ->", out / "icon.icns")
