import pickle
from pathlib import Path

import luigi
import pandas as pd
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import StandardScaler


def generate_summary_df(df_response: pd.DataFrame) -> pd.DataFrame:
    """
    解答データのresponse_seconds, responseについてuser_idごとの平均値を計算する。
    """
    return (
        df_response.groupby(by="user_id")
        .agg({"response_seconds": "mean", "response": "mean"})
        .reset_index()
        .rename(
            columns={
                "response_seconds": "average_response_seconds",
                "response": "correct_answer_rate",
            }
        )
    )


class LoadResponse(luigi.Task):
    """
    解答データをCSVから読み込む。
    """

    task_namespace = "load_data"

    def output(self):
        return luigi.LocalTarget(
            "./data/sample_pipeline/df_response.pkl", format=luigi.format.Nop
        )

    def run(self):
        df = pd.read_csv("./data/responses.csv")
        df.to_pickle(self.output().path)


class SummarizeResponseByUser(luigi.Task):
    """
    データをユーザ単位で集計する。
    """

    task_namespace = "preprocess"

    def requires(self):
        return LoadResponse()

    def output(self):
        return luigi.LocalTarget(
            "./data/sample_pipeline/df_summarized_by_user.pkl", format=luigi.format.Nop
        )

    def run(self):
        df_response = pd.read_pickle(self.input().path)
        df_summarized = (
            df_response.drop(columns=["question_id"])
            .groupby(by="user_id", as_index=False)
            .mean()
            .rename(
                columns={
                    "response_seconds": "average_response_seconds",
                    "response": "correct_answer_rate",
                }
            )
        )
        df_summarized.to_pickle(self.output().path)


class StandardizeData(luigi.Task):
    """
    データの標準化を行う。
    """

    task_namespace = "preprocess"

    def requires(self):
        return SummarizeResponseByUser()

    def output(self):
        return luigi.LocalTarget(
            "./data/sample_pipeline/df_standardized.pkl", format=luigi.format.Nop
        )

    def run(self):
        df_response = pd.read_pickle(self.input().path)

        X = df_response[["average_response_seconds", "correct_answer_rate"]].to_numpy()
        standard_scaler = StandardScaler()
        scaled_X = standard_scaler.fit_transform(X)
        df_response[
            ["scaled_average_response_seconds", "scaled_correct_answer_rate"]
        ] = scaled_X

        df_response.to_pickle(self.output().path)


class PerformAgglomerativeClustering(luigi.Task):
    """
    標準化したデータをもとに凝集型クラスタリングを行い2群に分ける。
    """

    task_namespace = "filtering"

    def requires(self):
        return StandardizeData()

    def output(self):
        return luigi.LocalTarget(
            "./data/sample_pipeline/df_agglomerative_clustering.pkl",
            format=luigi.format.Nop,
        )

    def run(self):
        df = pd.read_pickle(self.input().path)

        X = df[
            ["scaled_average_response_seconds", "scaled_correct_answer_rate"]
        ].to_numpy()
        agglomerative = AgglomerativeClustering(n_clusters=2)
        df["cluster_number"] = agglomerative.fit_predict(X)

        df.to_pickle(self.output().path)


class ListCarelessUsers(luigi.Task):
    """
    不注意解答者のuser_idだけのリストを作る。

    クラスタリングで分けた2つのクラスターのうち、
    不注意解答者のクラスターは平均解答時間の短い方とする。
    """

    task_namespace = "filtering"

    def requires(self):
        return PerformAgglomerativeClustering()

    def output(self):
        return luigi.LocalTarget(
            "./data/sample_pipeline/serious_users.pkl", format=luigi.format.Nop
        )

    def run(self):
        df = pd.read_pickle(self.input().path)

        # 平均解答時間の短いクラスターの方が不注意解答者のクラスターであるとみなして
        # どちらのクラスターが不注意解答者のクラスターかを判定する。
        careless_cluster_number = int(
            df.drop(columns="user_id")
            .groupby(by="cluster_number")
            .mean()["average_response_seconds"]
            .idxmin()
        )

        df["is_careless"] = df["cluster_number"].map(
            lambda x: x == careless_cluster_number
        )

        with self.output().open("w") as f:
            pickle.dump(df[df["is_careless"] == True]["user_id"].tolist(), f)


class CalcStats(luigi.Task):
    """
    不注意解答者を除いたデータを使って各question_idについて統計量を計算する。
    """

    task_namespace = "calc_stats"

    def requires(self):
        return {
            "careless_users": ListCarelessUsers(),
            "response": LoadResponse(),
        }

    def output(self):
        return luigi.LocalTarget(
            "./data/sample_pipeline/df_stats.pkl", format=luigi.format.Nop
        )

    def run(self):
        with self.input()["careless_users"].open("r") as f:
            careless_users = pickle.load(f)
        df_response = pd.read_pickle(self.input()["response"].path)

        df_stats = (
            df_response[~df_response["user_id"].isin(careless_users)]
            .groupby(by="question_id")
            .agg({"response": "mean", "response_seconds": "mean"})
            .reset_index()
        )
        df_stats.to_pickle(self.output().path)


if __name__ == "__main__":
    Path("./data/sample_pipeline").mkdir(exist_ok=True, parents=True)
    luigi.run()
