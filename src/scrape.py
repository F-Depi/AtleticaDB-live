from func_general import get_db_engine
from func_scrape import get_iscritti

with get_db_engine().connect() as conn:
    get_iscritti(conn, None)

