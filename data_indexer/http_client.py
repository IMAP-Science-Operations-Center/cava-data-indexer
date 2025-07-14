import time

import httpx

http_client = httpx.Client()

def get_with_retry(url, times: int = 5) -> httpx.Response:
    for i in range(times):
        try:
            return http_client.get(url, follow_redirects=True)
        except Exception as e:
            if i == times - 1:
                raise e
            print(f"Retrying get for url {url}; retry number {i+1}; exception {e}")
            time.sleep(2**i)
