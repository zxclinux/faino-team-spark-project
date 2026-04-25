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
    q13_monthly_review_growth,
    q14_review_seasonality,
    q15_useful_votes_by_stars,
    q16_star_distribution_by_state,
    q17_city_size_vs_stars,
    q18_popular_user_categories,
)


class Question:
    def __init__(self, name: str, query_fn, author: str, output_dir: str):
        self.name = name
        self.query_fn = query_fn
        self.author = author
        self.output_dir = output_dir

    def run(self, datasets: dict):
        print(f"\n{'=' * 60}")
        print(f"  {self.author} — {self.name}")
        print(f"{'=' * 60}")
        result = self.query_fn(datasets)
        result.explain(True)
        result.show(truncate=False)
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{self.output_dir}/{self.query_fn.__name__}"
        )
        return result


QUESTIONS = []

QUESTIONS.append(Question(
    "Топ-3 ресторани за к-стю відгуків у кожному штаті (лише відкриті)",
    q1_top3_restaurants_per_state,
    author="Dmytro Korolchuk",
    output_dir="/data/output/dk",
))

QUESTIONS.append(Question(
    "Бізнес з найвищим рейтингом у кожному штаті (>100 відгуків)",
    q2_highest_rated_per_state,
    author="Dmytro Korolchuk",
    output_dir="/data/output/dk",
))

QUESTIONS.append(Question(
    "Середній рейтинг: працюють у вихідні vs лише будні",
    q3_weekend_vs_weekday_rating,
    author="Dmytro Korolchuk",
    output_dir="/data/output/dk",
))

QUESTIONS.append(Question(
    "Середній рейтинг: безкоштовний WiFi vs без WiFi",
    q4_wifi_vs_no_wifi_rating,
    author="Dmytro Korolchuk",
    output_dir="/data/output/dk",
))

QUESTIONS.append(Question(
    "Залежність між кількістю чекінів і середнім рейтингом бізнесу",
    q5_checkins_vs_rating,
    author="Dmytro Korolchuk",
    output_dir="/data/output/dk",
))

QUESTIONS.append(Question(
    "Бізнеси з найбільшою розбіжністю між офіційним і середнім рейтингом відгуків",
    q6_rating_discrepancy,
    author="Dmytro Korolchuk",
    output_dir="/data/output/dk",
))

QUESTIONS.append(Question(
    "Середній рейтинг від активних оглядачів (>100 reviews) vs загальний середній",
    q7_active_reviewers_vs_all_avg_rating,
    author="Yaroslav Vasylyshyn",
    output_dir="/data/output/yv",
))

QUESTIONS.append(Question(
    "Порівняння середнього рейтингу від elite vs non-elite користувачів",
    q8_elite_vs_non_elite_rating,
    author="Yaroslav Vasylyshyn",
    output_dir="/data/output/yv",
))

QUESTIONS.append(Question(
    "Користувачі з друзями вище медіани vs решта: середній рейтинг",
    q9_median_friends_vs_rest_rating,
    author="Yaroslav Vasylyshyn",
    output_dir="/data/output/yv",
))

QUESTIONS.append(Question(
    "Moving average рейтингу по останніх 10 відгуках для кожного бізнесу",
    q10_business_moving_avg_last10_reviews,
    author="Yaroslav Vasylyshyn",
    output_dir="/data/output/yv",
))

QUESTIONS.append(Question(
    "Користувачі з >100 reviews, які жодного разу не отримали useful",
    q11_active_users_no_useful_votes,
    author="Yaroslav Vasylyshyn",
    output_dir="/data/output/yv",
))

QUESTIONS.append(Question(
    "Зв'язок між друзями користувача та avg useful на його review",
    q12_friends_vs_avg_useful_votes,
    author="Yaroslav Vasylyshyn",
    output_dir="/data/output/yv",
))

QUESTIONS.append(Question(
    "Місячна динаміка зростання кількості відгуків для Топ-10 найбільш оглянутих міст (MoM %)",
    q13_monthly_review_growth,
    author="Yurii Sirenko",
    output_dir="/data/output/ys",
))

QUESTIONS.append(Question(
    "Сезонність відгуків — рейтинг місяців за кількістю відгуків протягом року",
    q14_review_seasonality,
    author="Yurii Sirenko",
    output_dir="/data/output/ys",
))

QUESTIONS.append(Question(
    "Середня кількість useful голосів на відгуки для 5-зіркових vs 1-зіркових бізнесів",
    q15_useful_votes_by_stars,
    author="Yurii Sirenko",
    output_dir="/data/output/ys",
))

QUESTIONS.append(Question(
    "Розподіл зіркових рейтингів по штатах (лише відкриті бізнеси)",
    q16_star_distribution_by_state,
    author="Yurii Sirenko",
    output_dir="/data/output/ys",
))

QUESTIONS.append(Question(
    "Чи більші міста мають вищі рейтинги (лише відкриті бізнеси)",
    q17_city_size_vs_stars,
    author="Yurii Sirenko",
    output_dir="/data/output/ys",
))

QUESTIONS.append(Question(
    "Які категорії бізнесу оглядають популярні користувачі (fans > 10)",
    q18_popular_user_categories,
    author="Yurii Sirenko",
    output_dir="/data/output/ys",
))