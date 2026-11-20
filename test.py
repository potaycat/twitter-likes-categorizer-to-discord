import httpx, json
from app import AppConfig, build_oauth1_authorization_header
cfg = AppConfig.from_env()
for path in ("/2/webhooks", "/2/activity/subscriptions"):
    url = f"https://api.x.com{path}"
    hdr = build_oauth1_authorization_header("GET", url, cfg.x_consumer_key, cfg.x_consumer_secret, cfg.x_access_token, cfg.x_access_token_secret)
    r = httpx.get(url, headers={"Authorization": hdr})
    print(f"\n=== GET {path} ({r.status_code}) ===")
    print(json.dumps(r.json(), indent=2))
