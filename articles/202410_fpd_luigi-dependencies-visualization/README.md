# 機械学習パイプラインLuigiのタスク同士の関係を良い感じに可視化する方法

URL: https://blog.nnn.dev/entry/luigi-pipeline-visualization

## リソースの説明

| リソース                       | 説明                                                                                                             |
| :--------------------------- | :--------------------------------------------------------------------------------------------------------------- |
| `luigi_dependency_graphs.py` | 記事で紹介したものです。タスク情報の抽出、依存関係の解析、Graphvizを使用した図の生成を行う機能を提供しています。 |
| `sample_data_generator.py`   | サンプルデータとサンプルデータの散布図を生成するものです。                                                       |
| `sample_pipeline.py`         | 記事中で扱ったパイプラインを定義しています。                                                                     |
| `sample_visualization.py`    | `sample_pipeline.py` で定義したタスクの依存関係を可視化する例です。                                                 |
| `Pipfile`                    | Pythonプロジェクトの依存関係を管理するためのファイルです。                                                       |
| `Pipfile.lock`               | 依存関係をバージョン込みで記録しているファイルです。こちらを使うことで環境を再現できます。                       |
| `./data/`                    | サンプルデータ・タスクの依存関係の図・パイプラインの出力データの保存先です。                                     |


## 環境構築手順
### graphvizのインストール
graphvizが必要であるため、https://graphviz.org/download/ からインストールしてください。必要に応じてパスを通すなどしてください。

### pipenvのインストール
pipenvが必要であるため、https://pipenv-ja.readthedocs.io/ja/translate-ja/install.html#installing-pipenv の手順にしたがってインストールしてください。

### 必要なライブラリのインストール
このREADMEのあるディレクトリで以下のコマンドを実行して、ライブラリをインストールしてください。
```
pipenv sync
```

## 実行手順

### 可視化
このREADMEのあるディレクトリで以下のコマンドを実行して、Luigiタスクの依存関係を可視化した図を `./data/graphs/` 以下に出力してください。
```
pipenv run python sample_visualization.py
```

### Luigiパイプラインの実行

#### サンプルデータの生成
このREADMEのあるディレクトリで以下のコマンドを実行して、サンプルデータとデータを可視化した図を `./data/` 以下に出力してください。
```
pipenv run python sample_data_generator.py 
```
#### タスクスケジューラーの起動
このREADMEのあるディレクトリで以下のコマンドを実行して、タスクスケジューラーを起動してください。Web UIはデフォルトで `localhost:8082` でアクセス可能です。
```
pipenv run luigid
```

#### パイプラインの実行
タスクスケジューラーを起動した状態で、このREADMEのあるディレクトリで以下のコマンドを実行して、パイプラインで定義されたタスクを実行してください。
```
pipenv run python sample_pipeline.py calc_stats.CalcStats
```

以下のメッセージが出ていれば成功です。なお、パイプラインの各タスクの出力するデータは `./data/sample_pipeline/` 以下に作成されます。

```
===== Luigi Execution Summary =====

Scheduled 6 tasks of which:
* 6 ran successfully:
    - 1 calc_stats.CalcStats()
    - 1 filtering.ListCarelessUsers()
    - 1 filtering.PerformAgglomerativeClustering()
    - 1 load_data.LoadResponse()
    - 1 preprocess.StandardizeData()
    ...

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====
```

#### （Optional）Web UIからのタスク依存関係可視化
Web UI（デフォルト：`localhost:8082`）にアクセスして、`Dependency Graph` のタブを開きます。
パイプラインの実行時に以下のようなメッセージがターミナルに出ているはずですので、`Show task details` 横の窓にパイプラインの実行時に表示された `calc_stats.CalcStats__xxxxxxxxxx` を入力して、`Show task details` を押下します。
```
INFO: Informed scheduler that task   calc_stats.CalcStats__xxxxxxxxxx   has status   PENDING
```

画面上に依存関係のグラフが表示されますが、表示方法をシンプルなSVGとD3で切り替え可能です。