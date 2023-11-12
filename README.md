# TransJakarta Backend

## Development

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Start FastAPI process

```bash
uvicorn main:app --reload
```

3. Open local API docs [http://localhost:8000/docs](http://localhost:8000/docs)

## Docker

1. Run

```bash
docker compose up -d --build
```

2. .env for redis's host can't be localhost so change it to `REDIS_HOST=redis`
