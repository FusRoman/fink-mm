from fink_client.consumer import AlertConsumer
import tabulate

# used by the integration test to display distributed alerts
if __name__ == "__main__":
    maxtimeout = 10
    myconfig = {
        "username": "rlm",
        "bootstrap.servers": "localhost:9092",
        "group_id": "rlm_fink",
    }
    topics = ["fink_grb_bronze"]

    headers = [
        "Generated at (jd)",
        "Topic",
        "objectId",
        "Fink_Class",
        "Rate",
    ]

    consumer = AlertConsumer(topics, myconfig)
    topic, alert, key = consumer.poll(maxtimeout)

    table = [
        [alert["jd"], topic, alert["objectId"], alert["fink_class"], alert["rate"]]
    ]

    print(tabulate.tabulate(table, headers, tablefmt="pretty"))
