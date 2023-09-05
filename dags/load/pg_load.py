from typing import Iterator, Dict, Any, Optional
import psycopg2.extras
import io


def iter_cannabs_from_file(path: str) -> Iterator[Dict[str, Any]]:
    import json
    with open(path, 'r') as f:
        data = json.load(f)
        for cannab in data:
            yield cannab


def create_staging_table(cursor, datetime):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS cannabis (
            id                            integer,
            uid                           Character(36) not null,
            strain                        VARCHAR(50),
            cannabinoid_abbreviation      VARCHAR(10),
            cannabinoid                   VARCHAR(50),
            terpene                       VARCHAR(50),
            medical_use                   VARCHAR(50),
            health_benefit                TEXT,
            category                      VARCHAR(30),
            type                          VARCHAR(6),
            buzzword                      VARCHAR(30),
            brand                         VARCHAR(50),
            datetime_downloaded           timestamp not null,
            date_downloaded               date not null,
            PRIMARY KEY (uid)
        );
        DELETE FROM cannabis
        WHERE datetime_downloaded = '{datetime}';
    """)


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


class StringIteratorIO(io.TextIOBase):

    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def copy_string_iterator(connection, cannabs: Iterator[Dict[str, Any]], size: int = 8192, datetime: str = None, date: str = None) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor, datetime)

        cannabs_string_iterator = StringIteratorIO((
            ';'.join(map(clean_csv_value, (
                cannab['id'],
                cannab['uid'],
                cannab['strain'],
                cannab['cannabinoid_abbreviation'],
                cannab['cannabinoid'],
                cannab['terpene'],
                cannab['medical_use'],
                cannab['health_benefit'],
                cannab['category'],
                cannab['type'],
                cannab['buzzword'],
                cannab['brand'],
                datetime,
                date,
            ))) + '\n'
            for cannab in cannabs
        ))

        cursor.copy_from(cannabs_string_iterator, 'cannabis', sep=';', size=size)


def load_data(**context) -> None:
    connection_dict = context['params']['connection'].get_uri()
    connection = psycopg2.connect(connection_dict)
    connection.set_session(autocommit=True)

    cannabs = iter_cannabs_from_file(context['templates_dict']['filename'])

    copy_string_iterator(connection, cannabs, 1024, context['data_interval_end'], context['ds'])
