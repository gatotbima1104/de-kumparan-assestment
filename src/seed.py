import argparse
from datetime import date, datetime, time

from psycopg2.extensions import connection

from src.connections.db_connection import PostgresConnection
from src.constants import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from src.seeder.article_seeder import ArticleSeeder


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
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2016, 1, 1),
        help="Start date in YYYY-MM-DD format (default: 2016-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="End date in YYYY-MM-DD format (default: today)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.articles < 0 or args.authors < 0:
        raise SystemExit("Error: article and author counts cannot be negative.")

    if args.start_date > args.end_date:
        raise SystemExit("Error: start date cannot be after end date.")

    start_datetime = datetime.combine(args.start_date, time.min)
    end_datetime = datetime.combine(args.end_date, time.max)

    conn: connection = PostgresConnection(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
    ).get_connection()

    try:
        seeder = ArticleSeeder(
            conn=conn,
            n_articles=args.articles,
            n_authors=args.authors,
            start_date=start_datetime,
            end_date=end_datetime,
        )
        seeder.run()

    finally:
        conn.close()


if __name__ == "__main__":
    main()