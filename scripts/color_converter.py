import sys
import pyperclip

COLOR_MAP = {
    "513b3b": "963333ff",
    "51413b": "964c33ff",
    "4a4137": "744d27ff",
    "4a4637": "746027ff",
    "404434": "49571dff",
    "3e4535": "3f5e1fff",
    "3a4535": "2f5e1fff",
    "364736": "206320ff",
    "36473a": "206332ff",
    "354542": "1f5e4fff",
    "354545": "1f5e5eff",
    "354245": "1f4e5eff",
    "39444d": "2c5885ff",
    "393f4d": "2c4285ff",
    "3f3f59": "4141c4ff",
    "463f59": "6241c4ff",
    "4a3c54": "7038a8ff",
    "4f3c54": "8c38a8ff",
    "4f394e": "8a2d8aff",
    "4f394a": "8a2d74ff",
    "503a46": "913061ff",
    "503a40": "913048ff",
    "4d4d4c": "808080ff",
    "464646": "616161ff",
    "434342": "525252ff",
    "3f3f3f": "424242ff",
    "3b3b3b": "333333ff"
}

def main():
    print("hex入力(#なし、空で終了): ", end="", flush=True)
    try:
        for line in sys.stdin:
            input_hex = line.strip().lower()
            if not input_hex:
                break

            input_hex = input_hex.lstrip("#")

            out = COLOR_MAP.get(input_hex)
            if not out:
                print("変換先なし。")
            else:
                print(out)
                try:
                    pyperclip.copy(out)
                except Exception as e:
                    print(f"クリップボードへのコピーに失敗しました: {e}")

            print("hex入力(#なし、空で終了): ", end="", flush=True)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
