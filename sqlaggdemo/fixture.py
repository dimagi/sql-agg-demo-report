import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import *
from django.conf import settings

metadata = sqlalchemy.MetaData()

user_table = Table("user_table",
                   metadata,
                   Column("user", String(50), primary_key=True, autoincrement=False),
                   Column("date", DATE, primary_key=True, autoincrement=False),
                   Column("indicator_a", INT),
                   Column("indicator_b", INT),
                   Column("indicator_c", INT),
                   Column("indicator_d", INT)
)

region_table = Table("region_table",
                     metadata,
                     Column("region", String(50), primary_key=True, autoincrement=False),
                     Column("sub_region", String(50), primary_key=True, autoincrement=False),
                     Column("date", DATE, primary_key=True, autoincrement=False),
                     Column("indicator_a", INT),
                     Column("indicator_b", INT)
)


def main():
    engine = create_engine("postgresql+psycopg2://postgres:password@localhost/{domain}".format(domain="sqlagg-demo"))
    metadata.bind = engine
    user_table.drop(engine, checkfirst=True)
    region_table.drop(engine, checkfirst=True)
    metadata.create_all()

    user_data = [
        {"user": "user1", "date": "2013-01-01", "indicator_a": 1, "indicator_b": 0, "indicator_c": 1, "indicator_d": 1},
        {"user": "user1", "date": "2013-02-01", "indicator_a": 0, "indicator_b": 1, "indicator_c": 1, "indicator_d": 1},
        {"user": "user2", "date": "2013-01-01", "indicator_a": 0, "indicator_b": 1, "indicator_c": 1, "indicator_d": 2},
        {"user": "user2", "date": "2013-02-01", "indicator_a": 1, "indicator_b": 0, "indicator_c": 1, "indicator_d": 2},
    ]

    region_data = [
        {"region": "region1", "sub_region": "region1_a", "date": "2013-01-01", "indicator_a": 1, "indicator_b": 0},
        {"region": "region1", "sub_region": "region1_b", "date": "2013-01-01", "indicator_a": 0, "indicator_b": 1},
        {"region": "region1", "sub_region": "region1_a", "date": "2013-02-01", "indicator_a": 1, "indicator_b": 1},
        {"region": "region1", "sub_region": "region1_b", "date": "2013-02-01", "indicator_a": 0, "indicator_b": 0},
        {"region": "region2", "sub_region": "region2_a", "date": "2013-01-01", "indicator_a": 0, "indicator_b": 1},
        {"region": "region2", "sub_region": "region2_b", "date": "2013-01-01", "indicator_a": 1, "indicator_b": 0},
        {"region": "region2", "sub_region": "region2_a", "date": "2013-02-01", "indicator_a": 1, "indicator_b": 1},
        {"region": "region2", "sub_region": "region2_b", "date": "2013-02-01", "indicator_a": 0, "indicator_b": 0},
    ]

    connection = engine.connect()
    try:
        connection.execute(user_table.delete())
        connection.execute(region_table.delete())
        for d in user_data:
            insert = user_table.insert().values(**d)
            connection.execute(insert)

        for d in region_data:
            insert = region_table.insert().values(**d)
            connection.execute(insert)
    finally:
        connection.close()

if __name__ == "__main__":
    main()