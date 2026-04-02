from queries import (
    q1_top3_restaurants_per_state,
    q2_highest_rated_per_state,
    q3_weekend_vs_weekday_rating,
    q4_wifi_vs_no_wifi_rating,
    q5_checkins_vs_rating,
    q6_rating_discrepancy,
)


class Question:
    def __init__(self, tag: str, name: str, query_fn, author: str = "Dmytro Korolchuk"):
        self.name = name
        self.query_fn = query_fn
        self.author = author

    def run(self, datasets: dict, output_dir: str = "output/dk"):
        print(f"\n{'=' * 60}")
        print(f"  {self.author} — {self.name}")
        print(f"{'=' * 60}")
        result = self.query_fn(datasets)
        result.show(truncate=False)
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{output_dir}/{self.name}"
        )
        return result


QUESTIONS = []

QUESTIONS.append(Question(
    "Топ-3 ресторани за к-стю відгуків у кожному штаті (лише відкриті)",
    q1_top3_restaurants_per_state,
))

QUESTIONS.append(Question(
    "Бізнес з найвищим рейтингом у кожному штаті (>100 відгуків)",
    q2_highest_rated_per_state,
))

QUESTIONS.append(Question(
    "Середній рейтинг: працюють у вихідні vs лише будні",
    q3_weekend_vs_weekday_rating,
))

QUESTIONS.append(Question(
    "Середній рейтинг: безкоштовний WiFi vs без WiFi",
    q4_wifi_vs_no_wifi_rating,
))

QUESTIONS.append(Question(
    "Залежність між кількістю чекінів і середнім рейтингом бізнесу",
    q5_checkins_vs_rating,
))

QUESTIONS.append(Question(
    "Бізнеси з найбільшою розбіжністю між офіційним і середнім рейтингом відгуків",
    q6_rating_discrepancy,
))
