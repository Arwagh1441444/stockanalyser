# AR Tiger Tech Analysis — launch checklist

## Before deployment
- Put `app.py`, `refresh_engine.py`, `market_db.py`, `news_engine.py`, `universe_lists.py`, and `requirements.txt` in the repo root.
- In Streamlit secrets or environment variables, set:
  - `APP_USERNAME`
  - `APP_PASSWORD`
  - `NEWS_API_KEY`
  - optional: `MASTER_MARKET_DB`
- Run `python smoke_test.py` locally once.
- Run `python -m py_compile app.py refresh_engine.py market_db.py news_engine.py`.

## First refresh
- Start the app.
- Use `Quick` mode first.
- Confirm that `Provider Health` shows at least one successful market provider and one news provider row.
- Confirm the database file is created.

## Production notes
- The current NewsAPI developer key is fine for development and testing, but not for a public production deployment.
- For public launch, move the key out of code and into secrets.
- If you need truly live news, upgrade the NewsAPI plan or replace the news source.
- Start with `Standard` refresh mode only after the first quick refresh succeeds.

## Ongoing maintenance
- Refresh daily outside market hours for full history updates.
- Refresh intraday with a lower `news request budget` to stay under plan limits.
- Monitor `Provider Health` for repeated `FAILED` rows and lower `Active market refresh symbols` if the host is slow.
