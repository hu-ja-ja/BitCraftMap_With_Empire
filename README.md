# BitCraftMap_With_Empire

for [BitCraftMap](https://github.com/bitcraftmap/bitcraftmap)

本リポジトリは [BitJita](https://bitjita.com/docs/api) から Empire/Tower のデータを取得し、GeoJSON に変換するツール群を含みます。

<h3>閲覧方法</h3>

リポジトリは生成された GeoJSON を自動的に [Gist](https://gist.github.com/hu-ja-ja/0df91f5827d8bd8ade52d40ce4d5d20d) にアップロードする GitHub Actions を実行しており、出力は [BitCraftMap](https://bitcraftmap.com/?gistId=0df91f5827d8bd8ade52d40ce4d5d20d) 上で閲覧できます。

## 前提

このリポジトリは [`uv`](https://github.com/astral-sh/uv)を使用して依存を再現する前提での利用を想定しています。

[公式ドキュメント](https://docs.astral.sh/uv)

注意: `uv sync` を実行する前にご自身の環境が要件を満たすか確認してください。満たさない場合は `uv python install <version>` で適切な Python を用意してください。

## すばやく始める

1. `uv` をインストールしてください(OS に応じて公式ドキュメントに従ってください)。

2. プロジェクトルートで依存を同期します(`uv.lock` と `pyproject.toml` を参照して再現可能な環境が作られます):

```powershell
# プロジェクトルートで
uv sync
```

3. コマンドラインのエントリは `generate` コンソールスクリプトとしてパッケージに登録されています。`uv` 経由で実行してください(`uv` が仮想環境を管理します):

```powershell
# 直接実行(エイリアス `generate` を使用)
uv run generate

# ヘルプや引数付き実行
uv run generate --help
uv run generate --limit-empires 100 --verbose --out Resource/my.geojson
```

## スクリプトの使い方

パッケージに登録された CLI エントリ `generate` を `uv run` で呼び出してください。デフォルトでは `Resource/generated.geojson` に出力します。

基本実行例 (uv 経由):

```powershell
uv run generate
```

主要な引数(抜粋):

- `--out` : 出力先パス(デフォルト: `Resource/generated.geojson`)
- `--user-agent` : BitJita API に送る User-Agent ヘッダ(デフォルトはリポジトリ内定義)
- `--throttle-ms` : 各 API 呼び出し間の最小待ち時間(ミリ秒、デフォルト 120)
- `--limit-empires` : 処理するエンパイア数を制限(テスト用)
- `--max-features` : 出力する Feature の最大数(0 = 制限なし)
- `--max-towers-per-empire` : 各エンパイアで処理する塔の上限(0 = 制限なし)
- `--rate-per-min` : レートリミッタの設定(分あたりの許可リクエスト数)
- `--workers` : 並列ワーカ数(デフォルト 8)
- `--verbose` : 進捗ログを詳細に出力
- `--color-store` : エンティティID -> 色マップを格納する YAML ファイルのパス(デフォルト: `Resource/color_map.yaml`)

引数を指定した実行例 (uv 経由):

```powershell
uv run generate --limit-empires 100 --verbose --out Resource/my.geojson
```

## 出力について

- 出力は GeoJSON の FeatureCollection です。BitCraftMap の[フォーマット](https://github.com/bitcraftmap/bitcraftmap?tab=readme-ov-file#feature--custom-markers) と互換性を保つプロパティを付与しています。

## 注意点

- BitJita API にはレート制限があります。デフォルト設定でも過度な同時実行は避けてください。
- `uv.lock` / `pyproject.toml` に記載された Python バージョン要件を確認してください。必要なら `uv python install <version>` を使って適切な Python を用意してください。
- マッピングルールや座標変換の詳細は `要件.md` を参照してください。

## 追加ヘルプ

より詳しいオプションはスクリプトの `--help` をご利用ください:

```powershell
uv run generate --help
```
