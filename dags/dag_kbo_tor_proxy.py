"""Test KBO via proxies Tor (TaskFlow). Déclenchement manuel uniquement."""

from airflow.decorators import dag, task
from datetime import datetime
import random
import requests
import time

TOR_PROXIES = [
    "socks5h://tor1:9050",
    "socks5h://tor2:9050",
    "socks5h://tor3:9050",
]

URL = "https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?nummer=0200.065.765"
IP_CHECK_URL = "https://api.ipify.org?format=json"


@dag(
    dag_id="spam_kbo_tor_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tor", "kbo", "taskflow", "proxy"],
)
def spam_kbo_dag():

    @task
    def spam_kbo():

        success = 0
        failed = 0

        for i in range(10000):

            proxy = random.choice(TOR_PROXIES)

            proxies = {
                "http": proxy,
                "https": proxy,
            }

            try:
                # --- exit IP check ---
                ip_resp = requests.get(
                    IP_CHECK_URL,
                    proxies=proxies,
                    timeout=30,
                )
                exit_ip = ip_resp.json().get("ip", "unknown")

                # --- main request ---
                response = requests.get(
                    URL,
                    proxies=proxies,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 "
                            "(Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 "
                            "(KHTML, like Gecko) "
                            "Chrome/122 Safari/537.36"
                        )
                    },
                    timeout=60,
                )

                print(
                    f"[{i}] "
                    f"STATUS={response.status_code} "
                    f"PROXY={proxy} "
                    f"EXIT_IP={exit_ip}"
                )

                if response.status_code == 200:
                    success += 1
                else:
                    failed += 1

                if response.status_code in [403, 429]:
                    print("RATE LIMIT DETECTED")
                    print(response.text[:500])
                    break

                time.sleep(0.5)

            except Exception as e:
                failed += 1
                print(
                    f"[{i}] FAILED "
                    f"PROXY={proxy} "
                    f"ERROR={e}"
                )

        print(f"SUCCESS={success}")
        print(f"FAILED={failed}")

        return {"success": success, "failed": failed}

    spam_kbo()


dag = spam_kbo_dag()
