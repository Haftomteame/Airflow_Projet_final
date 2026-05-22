import time
import requests

URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer=402695993"
SEARCH_WORD = "Dubois"

session = requests.Session()

request_count = 0
start_time = time.time()

while True:
    try:
        t0 = time.time()

        response = session.get(URL, timeout=10)

        elapsed = time.time() - t0
        request_count += 1

        html = response.text

        found = SEARCH_WORD.lower() in html.lower()

        print(
            f"\n[{request_count}] "
            f"Status: {response.status_code} | "
            f"Time: {elapsed:.3f}s | "
            f"Size: {len(html)} bytes | "
            f"'{SEARCH_WORD}' found: {found}"
        )


    except Exception as e:
        print(f"Request failed: {e}")

    total_elapsed = time.time() - start_time
    rps = request_count / total_elapsed

    print(f"Average requests/sec: {rps:.2f}")
    print("-" * 80)

    # Optional throttle
    # time.sleep(0.1)