from queries_ys import (
    monthly_review_growth,
    review_seasonality,
    useful_votes_by_stars,
    star_distribution_by_state,
    city_size_vs_stars,
    popular_user_categories,
    top_tip_users,
)

OUTPUT_DIR = "/data/output"

QUESTIONS = []


class Question:
    def __init__(self, author, content, output_name, query):
        self.author = author
        self.content = content
        self.output_name = output_name
        self.query = query
        QUESTIONS.append(self)


Question("YS", "Місячна динаміка зростання кількості відгуків для Топ-10 найбільш оглянутих міст (MoM %)", "monthly_review_growth", monthly_review_growth)
Question("YS", "Сезонність відгуків — рейтинг місяців за кількістю відгуків протягом року", "review_seasonality", review_seasonality)
Question("YS", "Середня кількість useful голосів на відгуки для 5-зіркових vs 1-зіркових бізнесів", "useful_votes_by_stars", useful_votes_by_stars)
Question("YS", "Розподіл зіркових рейтингів по штатах (лише відкриті бізнеси)", "star_distribution_by_state", star_distribution_by_state)
Question("YS", "Чи більші міста мають вищі рейтинги (лише відкриті бізнеси)", "city_size_vs_stars", city_size_vs_stars)
Question("YS", "Які категорії бізнесу оглядають популярні користувачі (fans > 10)", "popular_user_categories", popular_user_categories)
Question("YS", "Топ-10 користувачів з найбільшою кількістю tip-повідомлень та їхній профіль", "top_tip_users", top_tip_users)
