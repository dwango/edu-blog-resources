"""
これを実行することで以下を出力できます。
- `NUM_USERS`のユーザが`NUM_QUESTIONS`の問題に解答したときの解答時間・正誤についてのデータ
- 平均回答時間と正答率と省力解答者であるか否かの散布図
"""

import random
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

OUTPUT_DIR = Path("./data/")
NUM_USERS = 10000
NUM_QUESTIONS = 20
RATIO_NOT_CARELESS = 0.8


def calculate_correct_response_probability(
    theta: float, a: float, b: float, c: float = 0.25
) -> float:
    """
    学力がthetaのときに、識別力a・困難度b・推測度cの問題に正答する確率を求める

    Args:
        theta (float): 学力
        a (float): 識別力
        b (float): 困難度
        c (float): 推測度

    Returns:
        float: 正答する確率
    """
    return c + (1 - c) / (1 + np.exp(-1.701 * a * (theta - b)))


def generate_dummy_responses(df_theta, df_question):
    response_data = []

    for _, student in df_theta.iterrows():
        user_id = student["user_id"]
        theta = student["theta"]

        for _, question in df_question.iterrows():
            question_id = question["question_id"]
            a = question["a"]
            b = question["b"]

            # 正答確率の計算
            proba = calculate_correct_response_probability(theta, a, b)

            # 解答の生成 (1: 正答, 0: 誤答)
            response = np.random.binomial(1, proba)

            response_data.append(
                {"user_id": user_id, "question_id": question_id, "response": response}
            )

    return pd.DataFrame(response_data)


def simulate_answering_minutes(theta, b, response):
    """
    適当に解答に何分かかるかをシミュレーションする。
    何か根拠があってこのような算出式にしているわけではないが、難易度 / 学力が高いほど解答時間が長くなりがちである。
    """
    converted_theta = max(1, 3 + theta)
    converted_b = max(1, 3 + b) * 2

    if int(response) == 1:
        minutes = abs(
            np.random.normal(loc=converted_b / converted_theta / 5 + 0.05, scale=0.3)
        )
    else:
        if random.random() < 0.4:
            minutes = abs(
                np.random.normal(
                    loc=converted_b / converted_theta / 2 + 0.05, scale=0.3
                )
            )
        else:
            minutes = abs(
                np.random.normal(
                    loc=converted_b / converted_theta / 5 + 0.05, scale=0.3
                )
            )
    return max(min(minutes, 10), 0.05)


def add_simulated_answering_minutes(df_response, df_theta, df_question):
    # 各 DataFrame を user_id や question_id をキーに結合
    df_combined = df_response.merge(df_theta, on="user_id").merge(
        df_question, on="question_id"
    )

    # 解答時間をシミュレーションして追加
    df_combined["response_minutes"] = df_combined.apply(
        lambda row: simulate_answering_minutes(row["theta"], row["b"], row["response"]),
        axis=1,
    )

    return df_combined


def main():
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    random.seed(42)
    np.random.seed(42)

    num_not_careless = int(NUM_USERS * RATIO_NOT_CARELESS)
    user_ids = [f"user_{str(i).zfill(5)}" for i in range(1, NUM_USERS + 1)]
    question_ids = [f"q_{str(i).zfill(3)}" for i in range(1, NUM_QUESTIONS + 1)]
    not_careless_users = random.sample(user_ids, k=num_not_careless)
    careless_users = sorted(list(set(user_ids) - set(not_careless_users)))

    df_theta = pd.DataFrame(
        {
            "user_id": not_careless_users,
            "theta": np.random.normal(scale=1, size=num_not_careless),
        }
    )

    df_question = pd.DataFrame(
        {
            "question_id": question_ids,
            "a": np.random.lognormal(sigma=1, size=NUM_QUESTIONS),
            "b": np.random.normal(size=NUM_QUESTIONS),
        }
    )

    df_not_careless = generate_dummy_responses(df_theta, df_question)
    df_not_careless_with_time = add_simulated_answering_minutes(
        df_not_careless, df_theta, df_question
    )

    index = pd.MultiIndex.from_product(
        [careless_users, df_question["question_id"]], names=["user_id", "question_id"]
    )
    df_careless_with_time = pd.DataFrame(index=index).reset_index()
    df_careless_with_time["response"] = df_careless_with_time["question_id"].map(
        lambda x: 1 if random.random() < 0.25 else 0
    )
    df_careless_with_time["response_minutes"] = df_careless_with_time["user_id"].map(
        lambda x: max(abs(np.random.normal(loc=10 / 60, scale=0.3)), 0.03)
    )

    df_dummy_response = pd.concat(
        [
            df_not_careless_with_time.assign(is_careless=False),
            df_careless_with_time.assign(is_careless=True),
        ]
    )[["user_id", "question_id", "response", "response_minutes", "is_careless"]]
    df_dummy_response["response_seconds"] = df_dummy_response["response_minutes"] * 60

    (
        df_dummy_response[["user_id", "question_id", "response", "response_seconds"]]
        .sort_values(by=["user_id", "question_id"])
        .to_csv(Path(OUTPUT_DIR, "responses.csv"), index=False)
    )

    df_visualize = (
        df_dummy_response.drop(columns=["question_id"])
        .groupby(by=["user_id", "is_careless"], as_index=False)
        .mean()
    )
    FIG_TITLE = "Impact of Respondent Carefulness on Response Time and Accuracy"
    sns.scatterplot(
        df_visualize,
        x="response_seconds",
        y="response",
        hue="is_careless",
        hue_order=[True, False],
        palette={True: "red", False: "blue"},
        alpha=0.1,
    )
    plt.ylabel("Correct Answer Rate")
    plt.xlabel("Average Response Time per Question [s]")
    plt.ylim(0, 1.05)
    plt.xlim(0, df_visualize["response_seconds"].max() * 1.05)
    plt.title(FIG_TITLE)
    plt.savefig(f'./data/{FIG_TITLE.replace(' ', '_')}.png')


if __name__ == "__main__":
    main()
