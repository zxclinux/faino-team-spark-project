from queries import (
    q1_top3_restaurants_per_state,
    q2_highest_rated_per_state,
    q3_weekend_vs_weekday_rating,
    q4_wifi_vs_no_wifi_rating,
    q5_checkins_vs_rating,
    q6_rating_discrepancy,
    q7_active_reviewers_vs_all_avg_rating,
    q8_elite_vs_non_elite_rating,
    q9_median_friends_vs_rest_rating,
    q10_business_moving_avg_last10_reviews,
    q11_active_users_no_useful_votes,
    q12_friends_vs_avg_useful_votes,
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

QUESTIONS.append(Question(
    "Середній рейтинг від активних оглядачів (>100 reviews) vs загальний середній",
    q7_active_reviewers_vs_all_avg_rating,
    author="Yaroslav Vasylyshyn"
))

QUESTIONS.append(Question(
    "Порівняння середнього рейтингу від elite vs non-elite користувачів",
    q8_elite_vs_non_elite_rating,
    author="Yaroslav Vasylyshyn"
))

QUESTIONS.append(Question(
    "Користувачі з друзями вище медіани vs решта: середній рейтинг",
    q9_median_friends_vs_rest_rating,
    author="Yaroslav Vasylyshyn"
))

QUESTIONS.append(Question(
    "Moving average рейтингу по останніх 10 відгуках для кожного бізнесу",
    q10_business_moving_avg_last10_reviews,
    author="Yaroslav Vasylyshyn"
))

QUESTIONS.append(Question(
    "Користувачі з >100 reviews, які жодного разу не отримали useful",
    q11_active_users_no_useful_votes,
    author="Yaroslav Vasylyshyn"
))

QUESTIONS.append(Question(
    "Зв'язок між друзями користувача та avg useful на його review",
    q12_friends_vs_avg_useful_votes,
    author="Yaroslav Vasylyshyn"
))