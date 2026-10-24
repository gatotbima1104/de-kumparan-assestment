from src.seeder.article_seeder import ArticleSeeder
from src.constants import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from src.connections.db_connection import PostgresConnection
from psycopg2.extensions import connection
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Seed PostgreSQL article")

    parser.add_argument(
        "--articles",
        type=int,
        default=100,
        help="Number of articles to generate (default: 100)",
    )
    parser.add_argument(
        "--authors",
        type=int,
        default=10,
        help="Number of authors to generate (default: 10)",
    )

    return parser.parse_args()


def main():

    args = parse_args()
    if args.articles < 0 or args.authors < 0:
        raise SystemExit("Error: article and author counts cannot be negative.")

    conn: connection = PostgresConnection(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
    ).get_connection()

    try:
        seeder = ArticleSeeder(
            conn=conn, n_articles=args.articles, n_authors=args.authors
        )
        seeder.run()

    finally:
        conn.close()


if __name__ == "__main__":
    main()


# python -m src.seed \
#     --articles 1000 \
#     --authors 50