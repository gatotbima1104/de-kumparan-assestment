import random
from datetime import datetime
from uuid import UUID

from faker import Faker
from psycopg2.extensions import connection

from src.models.article_model import Article, Status
from src.setup import CATEGORIES


class ArticleSeeder:
    def __init__(
        self,
        conn: connection,
        n_articles: int = 100,
        n_authors: int = 10,
        locale: str = "en_US",
        seed=42,
        start_date: datetime = datetime(2016, 1, 1),
        end_date: datetime | None = None,
    ):
        self.conn = conn
        self.fake = Faker(locale)
        self.n_authors = n_authors
        self.n_articles = n_articles
        self.start_date = start_date
        self.end_date = end_date or datetime.now()
        Faker.seed(seed)
        random.seed(seed)

    def _categories(self) -> dict[str, int]:
        with self.conn.cursor() as cursor:
            for cat in CATEGORIES:
                # seed categories defined on setup.py
                cursor.execute(
                    """
                    INSERT INTO categories (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                    """,
                    (cat,),
                )

            cursor.execute(
                """
                SELECT id, name
                FROM categories
                WHERE name = ANY(%s)
                """,
                (CATEGORIES,),
            )

            cat_map = {name: id for id, name in cursor.fetchall()}

            # return map of categories {"Technology": 1, "Sport": 2, etc}
            return cat_map

    def _authors(self) -> list[UUID]:
        with self.conn.cursor() as cursor:
            # generate random author
            for _ in range(self.n_authors):
                name = self.fake.name()
                email = self.fake.unique.email(domain="example.xyz")
                created_at = self.fake.date_time_between(
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

                # generate authors and insert to pg
                cursor.execute(
                    """
                    INSERT INTO authors (name, email, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (name, email, created_at),
                )

            cursor.execute("SELECT id from authors")  # get author_id
            authors_ids = [row[0] for row in cursor.fetchall()]  # [(1,), (2,)]

            # return ids
            return authors_ids

    def _article(self, author_id: UUID, category_id: int) -> Article:
        created_at = self.fake.date_time_between(
            start_date=self.start_date,
            end_date=self.end_date,
        )

        # random status
        status = random.choices(list(Status), weights=[30, 60, 10], k=1)[0]

        published_at = None
        if status != Status.DRAFT:
            published_at = self.fake.date_time_between(
                start_date=created_at,
                end_date=self.end_date,
            )

        updated_at = self.fake.date_time_between(
            start_date=created_at,
            end_date=self.end_date,
        )

        # simulate soft delete for article with 5% of total article
        deleted_at = None
        if random.random() < 0.05:
            deleted_at = self.fake.date_time_between(
                start_date=created_at,
                end_date=self.end_date,
            )

        # return Article(title, content, status, etc.)
        return Article(
            title=self.fake.bs().capitalize(),
            content=self.fake.paragraph(nb_sentences=8),
            status=status,
            category_id=category_id,
            author_id=author_id,
            created_at=created_at,
            deleted_at=deleted_at,
            published_at=published_at,
            updated_at=updated_at,
        )

    def run(self):
        try:
            category_ids = list(
                self._categories().values()
            )  # get values of categories [1,2,3,4]
            author_ids = self._authors()  # get author_ids [UUID, UUID]

            if not category_ids or not author_ids:
                raise ValueError(
                    "Authors and categories must exist for seeding articles."
                )

            articles = [  # list of [Article(), Article()]
                self._article(
                    author_id=random.choice(author_ids),
                    category_id=random.choice(category_ids),
                )
                for _ in range(self.n_articles)
            ]

            with self.conn.cursor() as cursor:
                # execute articles
                cursor.executemany(
                    """
                    INSERT INTO articles (
                        title,
                        content,
                        status,
                        author_id,
                        category_id,
                        published_at,
                        created_at,
                        updated_at,
                        deleted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [article.to_tuple() for article in articles],
                )

                # commit articles to db
                self.conn.commit()
                print(
                    f"Successfully inserted {self.n_articles} articles, "
                    f"{len(author_ids)} available authors, "
                    f"and {len(category_ids)} categories."
                )

        except Exception:
            # rollback if failed
            self.conn.rollback()
            raise
