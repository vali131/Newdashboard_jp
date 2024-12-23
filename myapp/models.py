from django.db import connections
from datetime import datetime


class PendingCountFetcher:
    @staticmethod
    def fetch_pending_counts():
        # Get today's date in YYYYMMDD format
        today_date = datetime.now().strftime('%Y%m%d')

        # Replace static date with `today_date`
        queries = {
            'p_and_g_jp_amazon': [
                {"query": f"SELECT COUNT(*) FROM finalmaster_{today_date} WHERE STATUS = 'Pending'",
                 "label": f"finalmaster_{today_date}"},
                {"query": "SELECT COUNT(*) FROM searchterms WHERE STATUS = 'Pending'", "label": "searchterms"}
            ],
            'p_and_g_jp_amazon_app': [
                {"query": "SELECT COUNT(*) FROM searchterms WHERE STATUS = 'Pending'", "label": "searchterms"}
            ],
            'p_and_g_jp_amazon_competitor': [
                {"query": f"SELECT COUNT(*) FROM finalmaster_{today_date} WHERE STATUS = 'Pending'",
                 "label": f"finalmaster_{today_date}"}
            ],
            'p_and_g_jp_amazon_x': [
                {"query": f"SELECT COUNT(*) FROM finalmaster_{today_date} WHERE STATUS = 'Pending'",
                 "label": f"finalmaster_{today_date}"},
                {"query": "SELECT COUNT(*) FROM searchterms_sos1 WHERE STATUS = 'Pending'",
                 "label": "searchterms_sos1"},
                {"query": "SELECT COUNT(*) FROM searchterms_sos2 WHERE STATUS = 'Pending'",
                 "label": "searchterms_sos2"},
                {"query": "SELECT COUNT(*) FROM searchterms_sos3 WHERE STATUS = 'Pending'", "label": "searchterms_sos3"}
            ],
            'p_and_g_jp_amazon_x_app': [
                {"query": "SELECT COUNT(*) FROM searchterms_sos1 WHERE STATUS = 'Pending'",
                 "label": "searchterms_sos1"},
                {"query": "SELECT COUNT(*) FROM searchterms_sos2 WHERE STATUS = 'Pending'",
                 "label": "searchterms_sos2"},
                {"query": "SELECT COUNT(*) FROM searchterms_sos3 WHERE STATUS = 'Pending'", "label": "searchterms_sos3"}
            ],
            'p_and_g_jp_askul': [
                {"query": f"SELECT COUNT(*) FROM finalmaster_{today_date} WHERE STATUS = 'Pending'",
                 "label": f"finalmaster_{today_date}"},
                {"query": "SELECT COUNT(*) FROM searchterms WHERE STATUS = 'Pending'", "label": "searchterms"}
            ],
        }

        results = {}
        for db_name, db_queries in queries.items():
            results[db_name] = []
            with connections[db_name].cursor() as cursor:
                for query_info in db_queries:
                    try:
                        cursor.execute(query_info["query"])
                        count = cursor.fetchone()[0]
                        results[db_name].append({"label": query_info["label"], "count": count})
                    except Exception as e:
                        results[db_name].append({"label": query_info["label"], "count": "Error: Table not found"})
        return results
